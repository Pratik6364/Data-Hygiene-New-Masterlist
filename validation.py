import re
import rapidfuzz
from rapidfuzz import process, fuzz
from typing import Dict, Any, Tuple, List, Set, Optional
from utils import get_nested_value
from database import get_db, MASTERLIST_COL


async def determine_field_types(db, mappings: Dict[str, str]) -> Dict[str, str]:
    """
    Scans ALL records in the ExecutionInfo collection to determine the data type
    of each mapped field using a SINGLE PASS over the collection.
    
    Rules:
    - Ignore null, None, or empty string values.
    - For each non-empty value, try to convert it to an integer.
    - If conversion fails (contains alphabets, special characters, or mixed values
      like '9654p'), immediately stop checking that field and classify it as 'STRING'.
    - If ALL non-empty values are successfully converted to integers, classify as 'INTEGER'.
    
    Returns: {"CPUModel": "STRING", "coreCount": "INTEGER", ...}
    """
    field_types: Dict[str, str] = {}
    
    # 1. Resolve actual mapping paths for metadata fields from the masterlist.
    #    build_mappings() stores "" for metadata fields that don't have a top-level mapping.
    #    We look up the actual paths from masterlist's data.metadata.mapping_<fieldName> entries.
    resolved_paths: Dict[str, str] = dict(mappings)  # Start with existing mappings
    
    cursor_ml = db[MASTERLIST_COL].find({"status": "Published", "data.metadata": {"$exists": True}})
    async for ml_doc in cursor_ml:
        meta = ml_doc.get("data", {}).get("metadata", {})
        if not isinstance(meta, dict):
            continue
        for k, v in meta.items():
            if k.startswith("mapping_"):
                field_name = k.replace("mapping_", "")
                # Only fill in if the field exists in mappings but has no path
                if field_name in resolved_paths and not resolved_paths[field_name]:
                    resolved_paths[field_name] = str(v)
    
    # 2. Pre-classify fields that can't be scanned from ExecutionInfo
    #    and build a set of fields that need scanning
    fields_to_scan: Dict[str, str] = {}  # field_name -> resolved_mapping_path
    
    for field_name in mappings.keys():
        mapping_path = resolved_paths.get(field_name, "")
        
        if not mapping_path:
            field_types[field_name] = "STRING"
        elif mapping_path.startswith("processor_details."):
            # External collection — can't scan from ExecutionInfo
            field_types[field_name] = "STRING"
        else:
            fields_to_scan[field_name] = mapping_path
    
    # 3. SINGLE PASS: Scan all ExecutionInfo documents once, checking all fields simultaneously
    #    Track per-field state: whether we've found any value, and if still potentially integer
    still_checking: Dict[str, bool] = {f: True for f in fields_to_scan}   # True = still might be INTEGER
    found_value: Dict[str, bool] = {f: False for f in fields_to_scan}     # True = at least one non-empty value found
    
    cursor = db['Executioninfo'].find({}, {"_id": 0}).limit(1000)
    
    async for doc in cursor:
        # If all fields have been resolved (all marked STRING via early stopping), stop scanning
        if not still_checking:
            break
        
        # Check each field that hasn't been resolved yet
        for field_name in list(still_checking.keys()):
            if not still_checking.get(field_name):
                continue
            
            mapping_path = fields_to_scan[field_name]
            
            # Extract value using the mapping path
            raw_val = get_nested_value(doc, mapping_path)
            
            # Also check the flattened key (some docs store dot-notation as literal keys)
            if raw_val is None and mapping_path in doc:
                raw_val = doc[mapping_path]
            
            # Skip null / None / empty values
            if raw_val is None:
                continue
            
            str_val = str(raw_val).strip()
            
            if str_val == "" or str_val.lower() in ["none", "nan", "null"]:
                continue
            
            found_value[field_name] = True
            
            # Try to convert to integer
            try:
                int(str_val)
            except (ValueError, TypeError):
                # Early stopping for this field: non-integer value found
                still_checking.pop(field_name)
                field_types[field_name] = "STRING"
    
    # 4. Finalize remaining fields that were never classified as STRING
    for field_name in fields_to_scan:
        if field_name not in field_types:
            if found_value.get(field_name):
                field_types[field_name] = "INTEGER"
            else:
                field_types[field_name] = "STRING"  # No values found at all
    
    print(f"Field type detection results: {field_types}")
    return field_types


 
 
# Conditional validation rules: fields that only apply when sutType matches a condition
# Format: {masterlist_type: {"condition": "equals"|"not_equals", "value": "cloud"}}
# If a type is NOT listed here, it is validated unconditionally (for all records).
CONDITIONAL_RULES = {
    "instanceType": {"field": "sutInstanceMetadata.sutType", "condition": "equals", "value": "cloud"}
}
 
 
async def build_mappings() -> Dict[str, str]:
    """
    Dynamically builds the MAPPINGS dict from the masterlist collection.
    Returns: {"CPUModel": "platformProfile.sut.Summary.Server.CPUModel", ...}
    """
    db = get_db()
    # 1. Discover top-level types and primary mappings
    pipeline = [
        {"$match": {"status": "Published"}},
        {"$group": {
            "_id": "$type",
            "mapping": {"$first": "$data.mapping"}
        }}
    ]
    mappings = {}
    async for doc in db[MASTERLIST_COL].aggregate(pipeline):
        ml_type = doc["_id"]
        mapping_path = doc.get("mapping")
       
        if ml_type and mapping_path:
            mappings[ml_type] = mapping_path
 
    # 2. Discover metadata-level mappings (e.g., BenchmarkType mapping inside Benchmark)
    # This allows the API to see them as distinct parameters even if they're embedded.
    cursor = db[MASTERLIST_COL].find({"status": "Published", "data.metadata": {"$exists": True}})
    async for doc in cursor:
        meta = doc.get("data", {}).get("metadata", {})
        if not isinstance(meta, dict):
            continue
           
        for k, v in meta.items():
            param_name = k
            if k.startswith("mapping_"):
                param_name = k.replace("mapping_", "")
           
            # Standardization of dash labels
            if param_name.lower() == "benchmarktype":
                param_name = "BenchmarkType"
            if param_name.lower() == "cloudprovider":
                param_name = "cloudProvider"
               
            if param_name not in mappings:
                # Store the value as the path if it started with mapping_
                mappings[param_name] = str(v) if k.startswith("mapping_") else ""
   
    return mappings
   
# Values that should be ignored when building search signatures to prevent 'null' noise from diluting scores
IGNORED_SIGNATURE_VALUES = {"", "none", "null", "nan", "-", "na", "n/a", "undefined"}


class Validator:
    def __init__(self, ml_records, mappings: Dict[str, str], field_types: Dict[str, str] = None):
        self.mappings = mappings
        self.field_types = field_types or {}  # {"coreCount": "INTEGER", "CPUModel": "STRING", ...}
       
        self.valid_values: Dict[str, Set[str]] = {t: set() for t in mappings}
        self.value_ids: Dict[str, Dict[str, str]] = {t: {} for t in mappings}
        self.record_signatures: Dict[str, List[Dict]] = {t: [] for t in mappings}
        self.val_metadata_reqs: Dict[str, Dict[str, List[Dict]]] = {t: {} for t in mappings}
        self.all_metadata_values: Dict[str, Dict[str, str]] = {}
        self.type_metadata_paths: Dict[str, Dict[str, str]] = {t: {} for t in mappings}
       
        # Cache for processor details to avoid redundant DB lookups during validation
        # Only ~500 records exist, so we cache the entire collection for O(1) lookups.
        self.processor_cache: Dict[str, Dict[str, Any]] = {}
       
        # Track which types are primary (explicitly defined in masterlist as type)
        self.primary_types: Set[str] = set()
       
        for record in ml_records:
            t = record.get("type")
            data = record.get("data", {})
            val = str(data.get("value", "")).strip()
           
            if t == "InstanceType":
                t = "instanceType"
           
            if not t:
                continue
               
            self.primary_types.add(t)
           
            if t not in self.valid_values:
                continue
           
            masterlist_id = str(record.get("`_id`") or record.get("id") or str(record.get("_id", "")))
            if isinstance(masterlist_id, dict) and "$oid" in masterlist_id:
                masterlist_id = masterlist_id["$oid"]

            if val:
                # Normalize INTEGER-type values to canonical integer string (strip leading zeros, etc.)
                normalized_val = self._normalize_value(t, val)
                self.valid_values[t].add(normalized_val)
                self.value_ids[t][normalized_val] = masterlist_id
                if normalized_val not in self.val_metadata_reqs[t]:
                    self.val_metadata_reqs[t][normalized_val] = []
           
            meta_record = {}
            meta = data.get("metadata", {})
            normalized_val = self._normalize_value(t, val) if val else val
            signature_parts = [normalized_val] if normalized_val else []

            if isinstance(meta, dict):
                # We sort metadata keys alphabetically to ensure consistent signature generation
                for mk in sorted(meta.keys()):
                    if mk.startswith("mapping_") or mk == "mapping":
                        continue
                    
                    mv = meta[mk]
                    lookup_key = f"mapping_{mk}".lower()
                    meta_mapping_path = None
                   
                    for k, v in meta.items():
                        if k.lower() == lookup_key:
                            meta_mapping_path = v
                            break
                   
                    if not meta_mapping_path:
                        meta_mapping_path = meta.get("mapping", "")
                   
                    mv_str = str(mv).strip()
                    if mv_str:
                        # Normalize metadata values if the metadata field is INTEGER type
                        normalized_mv = self._normalize_value(mk, mv_str)
                        meta_record[mk] = {"mapping": meta_mapping_path, "required_val": normalized_mv}
                        signature_parts.append(normalized_mv)
                       
                        if mk not in self.all_metadata_values:
                            self.all_metadata_values[mk] = {}
                        self.all_metadata_values[mk][normalized_mv] = masterlist_id
                       
                        if meta_mapping_path:
                            self.type_metadata_paths[t][mk] = meta_mapping_path
           
            # Create a Mega-String Signature for this specific configuration
            # Exclude 'null', 'none', and other noise tokens to ensure clean matching
            signature_parts = [p for p in signature_parts if p.lower() not in IGNORED_SIGNATURE_VALUES]
            full_signature = " ".join(signature_parts).lower()
            
            if val:
                self.val_metadata_reqs[t][normalized_val].append((masterlist_id, meta_record))
                self.record_signatures[t].append({
                    "signature": full_signature,
                    "record_id": masterlist_id,
                    "primary_value": normalized_val,
                    "metadata": {mk: mv["required_val"] for mk, mv in meta_record.items()}
                })
 
    def _normalize_value(self, field_name: str, value: str) -> str:
        """
        Normalize a value based on its determined data type.
        For INTEGER fields: strip whitespace and convert to canonical integer string.
        For STRING fields: strip whitespace only.
        """
        stripped = str(value).strip()
        if self.field_types.get(field_name) == "INTEGER":
            try:
                return str(int(stripped))
            except (ValueError, TypeError):
                return stripped
        return stripped

    def get_suggestions(self, field_type: str, value: str, n: int = 3) -> List[Dict[str, Any]]:
        is_metadata = False
        possibilities = list(self.valid_values.get(field_type, set()))
        if not possibilities:
            possibilities = list(self.all_metadata_values.get(field_type, {}).keys())
            is_metadata = True
           
        if not possibilities or not value:
            return []
       
        matches = process.extract(value, possibilities, limit=n, scorer=fuzz.partial_ratio, score_cutoff=10)
        results = []
        for i, match_info in enumerate(matches, 1):
            match, score, _ = match_info
           
            if is_metadata:
                match_id = self.all_metadata_values.get(field_type, {}).get(match, "")
            else:
                match_id = self.value_ids.get(field_type, {}).get(match, "")
               
            results.append({
                f"suggestion{i}": match,
                f"score{i}": round(score / 100.0, 4),
                "status": "PENDING",
                "_id": match_id
            })
        return results
 
    def get_record_level_suggestions(self, field_type: str, value: str, actual_metadata: Dict[str, str] = None, n: int = 3) -> List[Dict[str, Any]]:
        """
        Returns top N record-level suggestions using 'Mega-String' concatenated matching.
        Combines primary value and metadata into a single string for high-accuracy fuzzy matching.
        """
        if actual_metadata is None:
            actual_metadata = {}
        
        type_configs = self.record_signatures.get(field_type, [])
        if not type_configs or (not value and not actual_metadata):
            return []
            
        # 1. Build the 'Mega-String' signature for our ACTUAL record
        # We include metadata values that exist to strengthen the search, but exclude null/none noise
        actual_signature_parts = [str(value).strip()]
        for m_name in sorted(actual_metadata.keys()):
            m_val = str(actual_metadata.get(m_name, "")).strip()
            if m_val:
                actual_signature_parts.append(m_val)
        
        # Clean the signature of null, none, -, and nan tokens
        cleaned_parts = [p for p in actual_signature_parts if p.lower() not in IGNORED_SIGNATURE_VALUES]
        actual_signature = " ".join(cleaned_parts).lower()
        
        # 2. Extract signatures for matching
        signature_strings = [c["signature"] for c in type_configs]
        
        # 3. Perform Fuzzy Search using token_set_ratio on Mega-Strings
        # This handles noisy names effectively by comparing intersections to the full token sets.
        matches = process.extract(actual_signature, signature_strings, limit=n, scorer=fuzz.token_set_ratio, score_cutoff=75)
        
        results = []
        for match_str, score, index in matches:
            config = type_configs[index]
            results.append({
                "_id": config["record_id"],
                "primary_value": config["primary_value"],
                "metadata": config["metadata"],
                "score": round(score / 100.0, 4)
            })
            
        return results

    def has_suggestions(self, field_type: str, value: str, actual_metadata: Dict[str, str] = None) -> bool:
        """
        Fast-path check: returns True if any record-level fuzzy match exists with score >= 75.
        Optimized by stopping at the first match found.
        """
        if actual_metadata is None:
            actual_metadata = {}
        
        type_configs = self.record_signatures.get(field_type, [])
        if not type_configs:
            return False
            
        actual_signature_parts = [str(value).strip()]
        for m_name in sorted(actual_metadata.keys()):
            m_val = str(actual_metadata.get(m_name, "")).strip()
            if m_val:
                actual_signature_parts.append(m_val)
        
        cleaned_parts = [p for p in actual_signature_parts if p.lower() not in IGNORED_SIGNATURE_VALUES]
        actual_signature = " ".join(cleaned_parts).lower()
        if not actual_signature:
            return False
            
        # Perform Fuzzy Search existence check (extractOne is fastest)
        match = process.extractOne(actual_signature, [c["signature"] for c in type_configs], scorer=fuzz.token_set_ratio, score_cutoff=75)
        
        return match is not None

    async def validate_doc(self, db, doc: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        # 1. Extract values for each mapped field (normalize based on detected type)
        field_values: Dict[str, str] = {}
        for t, path in self.mappings.items():
            raw_val = str(get_nested_value(doc, path) or '').strip()
            field_values[t] = self._normalize_value(t, raw_val)
       
        # 2. Determine which fields should be validated based on conditional rules
        should_validate: Dict[str, bool] = {}
        for t in self.mappings:
            if t in CONDITIONAL_RULES:
                rule = CONDITIONAL_RULES[t]
                condition_val = str(get_nested_value(doc, rule["field"]) or '').strip().lower()
                if rule["condition"] == "equals":
                    should_validate[t] = (condition_val == rule["value"])
                elif rule["condition"] == "not_equals":
                    should_validate[t] = (condition_val != rule["value"])
                else:
                    should_validate[t] = True
            else:
                should_validate[t] = True
       
        # 3. Basic validation: valid or invalid only
        field_status: Dict[str, str] = {}
        param_flags: Dict[str, bool] = {}
       
        for t in self.mappings:
            if t not in self.primary_types:
                continue # Skip metadata fields from top-level validation
               
            val = field_values[t]
            is_empty = (val == "" or val == "nan")
           
            if not should_validate[t]:
                field_status[t] = "valid"
                param_flags[t] = False
                continue
           
            if is_empty or val not in self.valid_values.get(t, set()):
                field_status[t] = "invalid"
                param_flags[t] = True
            else:
                field_status[t] = "valid"
                param_flags[t] = False
       
        # 5. Construct invalid payload
        invalid_payload = []
        for t in self.mappings:
            if t not in self.primary_types:
                continue # Skip metadata fields from top-level loop
               
            val = field_values[t]
            is_empty = (val == "" or val == "nan")
           
            t_metadata = []
            has_metadata_mismatch = False
           
            overall_field_status = field_status.get(t, "valid")
            primary_validation_status = "valid" if not param_flags.get(t, False) else "invalid"
           
            if not param_flags.get(t, False) and should_validate.get(t, True) and not is_empty:
                possible_configs = self.val_metadata_reqs.get(t, {}).get(val, [])
               
                if possible_configs:
                    best_config_metadata = []
                    min_errors = 999
                    perfect_match_found = False
                   
                    for record_id, config in possible_configs:
                        current_config_metadata = []
                        current_errors = 0
                       
                        for m_name, m_info in config.items():
                            m_path = m_info.get("mapping", "")
                            m_required_val = m_info.get("required_val", "")
                           
                            m_val = ""
                            if m_path.startswith("processor_details."):
                                target_field = m_path.split(".", 1)[1]
                                cpu_model_val = field_values.get("CPUModel", "")
                                if cpu_model_val:
                                    proc_doc = self.processor_cache.get(cpu_model_val)
                                    if proc_doc:
                                        m_val = str(proc_doc.get(target_field, "")).strip()
                            else:
                                m_val = str(get_nested_value(doc, m_path) or '').strip() if m_path else ""
                            
                            # Normalize metadata value based on detected type
                            m_val = self._normalize_value(m_name, m_val)
                           
                            m_is_empty = (m_val == "" or m_val == "nan")
                            if m_is_empty:
                                m_status = "invalid"
                                current_errors += 1
                            elif m_val != m_required_val:
                                m_status = "invalid"
                                current_errors += 1
                            else:
                                m_status = "valid"
                           
                            current_config_metadata.append({
                                "name": m_name,
                                "value": m_val,
                                "validation_status": m_status,
                                "mapping": m_path
                            })
                       
                        if current_errors == 0:
                            t_metadata = current_config_metadata
                            perfect_match_found = True
                            break
                       
                        if current_errors < min_errors:
                            min_errors = current_errors
                            best_config_metadata = current_config_metadata
                   
                    if not perfect_match_found:
                        t_metadata = best_config_metadata
                        has_metadata_mismatch = True
                        overall_field_status = "invalid"
           
            elif param_flags.get(t, False) and should_validate.get(t, True) and not is_empty:
                schema_reqs = self.type_metadata_paths.get(t, {})
                for m_name, m_path in schema_reqs.items():
                    m_val = ""
                    if m_path.startswith("processor_details."):
                        target_field = m_path.split(".", 1)[1]
                        cpu_model_val = field_values.get("CPUModel", "")
                       
                        if cpu_model_val:
                            proc_doc = self.processor_cache.get(cpu_model_val)
                            if proc_doc:
                                m_val = str(proc_doc.get(target_field, "")).strip()
                    else:
                        m_val = str(get_nested_value(doc, m_path) or '').strip() if m_path else ""
                    
                    # Normalize metadata value based on detected type
                    m_val = self._normalize_value(m_name, m_val)
                       
                    m_is_empty = (m_val == "" or m_val == "nan")
                    m_status = "invalid"
                   
                    t_metadata.append({
                        "name": m_name,
                        "value": m_val,
                        "validation_status": m_status,
                        "mapping": m_path
                    })
           
            if overall_field_status == "invalid":
                invalid_payload.append({
                    "field": t,
                    "value": val,
                    "validation_status": primary_validation_status,
                    "mapping": self.mappings.get(t, ""),
                    "metadata": t_metadata
                })
       
        return invalid_payload, field_status
 
import time
_validator_cache = {"instance": None, "updated_at": 0}
CACHE_TTL = 300  # 5 minutes

async def get_validator() -> Validator:
    global _validator_cache
    now = time.time()
    if _validator_cache["instance"] and (now - _validator_cache["updated_at"]) < CACHE_TTL:
        return _validator_cache["instance"]
        
    db = get_db()
    mappings = await build_mappings()
    
    # Determine field types by scanning all ExecutionInfo records
    field_types = await determine_field_types(db, mappings)
    
    ml_records = await db[MASTERLIST_COL].find({"status": "Published"}).to_list(length=None)
    validator = Validator(ml_records, mappings, field_types)
    
    # Pre-populate the processor cache
    cursor_proc = db["processor_details"].find({})
    async for proc in cursor_proc:
        model_no = proc.get("cpuModelNo")
        if model_no:
            validator.processor_cache[str(model_no)] = proc
    
    _validator_cache["instance"] = validator
    _validator_cache["updated_at"] = now
    return validator