import { useState, useEffect, useCallback } from "react";
import { API_URL } from "../config";
import { STATUS } from "../utils/correctionsTableConstants";
 
export const useCorrectionsTable = (data, history, execID, sutType, fetchData, showNotification) => {
  const [selectedSuggestions, setSelectedSuggestions] = useState({});
  const [editedSuggestions, setEditedSuggestions] = useState({});
  const [customSuggestions, setCustomSuggestions] = useState({});
  const [isAccepting, setIsAccepting] = useState(false);
  const [expandedGroups, setExpandedGroups] = useState(() =>
    Object.fromEntries(
      (data ?? []).map((group, i) => {
        const status = group.currentStatus?.toLowerCase();
        const isPending = !status || status === "invalid";
        return [i, isPending];
      })
    )
  );

  const [acceptConfirm, setAcceptConfirm] = useState(null);

  // Draft dialog state
  const [draftDialog, setDraftDialog] = useState(null);
  const [draftFields, setDraftFields] = useState([]);
  const [loadingDraftFields, setLoadingDraftFields] = useState(false);
  const [submittingDraft, setSubmittingDraft] = useState(false);

  // L0 confirm dialog state
  const [l0ConfirmDialog, setL0ConfirmDialog] = useState(null);
  const [submittingL0, setSubmittingL0] = useState(false);

  // ── History lookup helpers ─────────────────────────────────────
  // Structure:
  //   history.changes = [
  //     {
  //       field: "instanceType",
  //       changes: [
  //         { field: "CPUModel", from: "AMD Genoa", to: "7713" },
  //         { field: "instanceType", from: "c3d-standard-16", to: "Standard_Dadsv5.256" },
  //       ]
  //     }, ...
  //   ]

  /**
   * Find the history entry matching primaryField and return its changes array.
   */
  const getHistoryChangesForField = useCallback((primaryField) => {
    // Handle both { changes: [...] } and simply [...]
    const changesArr = Array.isArray(history) ? history : history?.changes;
    if (!Array.isArray(changesArr)) return null;
    
    const entry = changesArr.find((e) => e.field?.toLowerCase() === primaryField?.toLowerCase());
    if (!entry?.changes?.length) return null;
    return entry.changes;
  }, [history]);

  /**
   * Convert a changes array into a display object { field: to }.
   * Shows everything as-is, no filtering.
   */
  const buildDisplayObjectFromChanges = useCallback((changesArr) => {
    if (!changesArr?.length) return null;
    return Object.fromEntries(changesArr.map((c) => [c.field, c.to]));
  }, []);

  // Auto-selection logic
  useEffect(() => {
    if (!data || data.length === 0) return;
    if (Object.keys(selectedSuggestions).length > 0) return;

    const initialSelections = {};
    const initialCustom = {};

    data.forEach((group, groupIdx) => {
      const gStatus = group.currentStatus;
      if (gStatus !== STATUS.ACCEPTED && gStatus !== STATUS.APPROVED) return;

      const primaryField = group.invalid_field;

      // 1. First choice: Any suggestion explicitly marked as accepted
      const acceptedIdx = group.suggestions?.findIndex(
        (s) => s.status?.toLowerCase() === STATUS.ACCEPTED.toLowerCase()
      );
      if (acceptedIdx !== undefined && acceptedIdx !== -1) {
        initialSelections[groupIdx] = acceptedIdx;
        return;
      }

      // 2. Second choice: History changes as custom selection
      const changesArr = getHistoryChangesForField(primaryField);
      if (changesArr) {
        const displayObj = buildDisplayObjectFromChanges(changesArr);
        if (displayObj && Object.keys(displayObj).length > 0) {
          initialSelections[groupIdx] = "custom";
          initialCustom[groupIdx] = displayObj;
          return;
        }
      }
    });

    if (Object.keys(initialSelections).length > 0) setSelectedSuggestions(initialSelections);
    if (Object.keys(initialCustom).length > 0) setCustomSuggestions(initialCustom);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, history]);
 
  const handleSelect = useCallback((groupIdx, suggIdx) => {
    setSelectedSuggestions((prev) => {
      if (prev[groupIdx] === suggIdx) {
        const next = { ...prev };
        delete next[groupIdx];
        return next;
      }
      return { ...prev, [groupIdx]: suggIdx };
    });
    setEditedSuggestions((prev) => {
      const next = { ...prev };
      delete next[groupIdx];
      return next;
    });
  }, []);
 
  const handleSelectCustom = useCallback((groupIdx) => {
    setSelectedSuggestions((prev) => {
      const next = { ...prev };
      if (next[groupIdx] === "custom") {
        delete next[groupIdx];
      } else {
        next[groupIdx] = "custom";
      }
      return next;
    });
    setEditedSuggestions((prev) => {
      const next = { ...prev };
      delete next[groupIdx];
      return next;
    });
  }, []);
 
  const handleClearCustom = useCallback((groupIdx) => {
    setSelectedSuggestions((prev) => {
      const next = { ...prev };
      if (next[groupIdx] === "custom") delete next[groupIdx];
      return next;
    });
    setCustomSuggestions((prev) => {
      const next = { ...prev };
      delete next[groupIdx];
      return next;
    });
  }, []);
 
  const handleCustomMetadataFetch = useCallback((groupIdx, meta) => {
    setCustomSuggestions((prev) => ({ ...prev, [groupIdx]: meta }));
  }, []);
 
  const handleEditField = useCallback((groupIdx, key, newValue) => {
    setEditedSuggestions((prev) => ({
      ...prev,
      [groupIdx]: { ...(prev[groupIdx] || {}), [key]: newValue },
    }));
  }, []);

  const toggleGroup = useCallback(
    (idx) => setExpandedGroups((prev) => ({ ...prev, [idx]: !prev[idx] })),
    []
  );

  // ── Accept ────────────────────────────────────────────────────
  const handleAcceptConfirm = async () => {
    const { group, groupIdx } = acceptConfirm;
    const suggIdx = selectedSuggestions[groupIdx];
    if (suggIdx === undefined) return;

    const baseChosen =
      suggIdx === "custom"
        ? customSuggestions[groupIdx] || {}
        : group.suggestions[suggIdx];

    const customEdits = editedSuggestions[groupIdx] || {};
    const merged = { ...baseChosen, ...customEdits };
    const primaryField = group.invalid_field;
    const value = merged?.[primaryField] || merged?.[primaryField?.toLowerCase()];
 
    if (!value) return;
 
    const payload = {
      execution_id: execID,
      field_name: primaryField,
      accepted_value: value,
      currentStatus: STATUS.ACCEPTED,
    };
 
    if (sutType?.toLowerCase() === "vm") {
      // Prioritize CPU(s) from history
      const historyChanges = getHistoryChangesForField(primaryField);
      const historyCpu = historyChanges?.find((c) => c.field?.toLowerCase() === "cpu(s)");
      
      let cpusVal = historyCpu?.to;
      
      // Fallback to merged suggestion/custom value if not in history
      if (cpusVal === undefined) {
        cpusVal = merged?.["cpu(s)"] || merged?.["CPU(s)"] || merged?.["CPU(S)"];
      }

      if (cpusVal !== undefined) {
        // CPU(s) is an integer field, cast to number
        payload["CPU(s)"] = isNaN(Number(cpusVal)) ? cpusVal : Number(cpusVal);
      }
    }

    try {
      setIsAccepting(true);
      const res = await fetch(`${API_URL}/approve-suggestion`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok || data.status === "error") {
        showNotification(data?.message, "error");
        setAcceptConfirm(null);
        return;
      }
      setSelectedSuggestions((prev) => {
        const next = { ...prev };
        delete next[groupIdx];
        return next;
      });
      console.log(payload)
      setAcceptConfirm(null);
      fetchData();
      showNotification("Data accepted successfully", "success");
    } catch (err) {
      console.error(err);
      showNotification("Failed to accept data", "error");
    } finally {
      setIsAccepting(false);
    }
  };

  // ── L0 ────────────────────────────────────────────────────────
  const openL0Confirm = useCallback((group, groupIdx) => {
    setL0ConfirmDialog({ group, groupIdx });
  }, []);

  const handleL0Confirm = useCallback(async () => {
    setSubmittingL0(true);
    try {
      await fetch(`${API_URL}/reject-record`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ execution_id: execID, currentStatus: "L0 Data" }),
      });
      setL0ConfirmDialog(null);
      fetchData();
      showNotification("Rejected due to L0 data", "success");
    } catch (error) {
      console.error("Reject L0 API failed:", error);
      showNotification("Failed to send to L0", "error");
    } finally {
      setSubmittingL0(false);
    }
  }, [execID, fetchData, showNotification]);

  // ── Draft ─────────────────────────────────────────────────────
  const [draftInitialValues, setDraftInitialValues] = useState({});

  const openDraftDialog = useCallback(
    async (group, groupIdx) => {
      setDraftDialog({ group, groupIdx });
      setLoadingDraftFields(true);
      
      // Pre-fill from history
      const historyArr = getHistoryChangesForField(group.invalid_field);
      const historyObj = buildDisplayObjectFromChanges(historyArr) || {};
      setDraftInitialValues(historyObj);

      try {
        const res = await fetch(
          `${API_URL}/draft-records/fields?type=${encodeURIComponent(group.invalid_field)}`
        );
        const json = await res.json();
        setDraftFields(json.fields ?? []);
      } catch (error) {
        console.error("Fetch draft fields failed:", error);
        setDraftFields([]);
      } finally {
        setLoadingDraftFields(false);
      }
    },
    [getHistoryChangesForField, buildDisplayObjectFromChanges]
  );



  const handleDraftSubmit = useCallback(async (formValues) => {
    const { group } = draftDialog;

    // ── Validate integer fields before submitting ──────────────────
    const integerErrors = [];
    for (const field of draftFields) {
      if (field.datatype === "integer") {
        const val = formValues[field.fieldname];
        if (val === undefined || val === "") continue; // let allFilled handle blank
        if (!Number.isInteger(Number(val)) || isNaN(Number(val)) || String(val).includes(".") || Number(val) < 0) {
          integerErrors.push(field.fieldname);
        }
      }
    }
    if (integerErrors.length > 0) {
      showNotification(
        `The following fields require whole-number (integer) values: ${integerErrors.join(", ")}`,
        "error"
      );
      return;
    }

    // ── Build typed payload ────────────────────────────────────────
    const typedValues = {};
    for (const field of draftFields) {
      const raw = formValues[field.fieldname];
      if (raw !== undefined && raw !== "") {
        typedValues[field.fieldname] = field.datatype === "integer" ? Number(raw) : raw;
      } else {
        typedValues[field.fieldname] = raw ?? "";
      }
    }

    setSubmittingDraft(true);
    try {
      const payload = {
        execution_id: execID,
        ...typedValues,
        currentStatus: "On Hold",
      };

      if (sutType?.toLowerCase() === "vm") {
        const historyChanges = getHistoryChangesForField(group.invalid_field);
        const historyCpu = historyChanges?.find((c) => c.field?.toLowerCase() === "cpu(s)");
        if (historyCpu?.to !== undefined) {
          payload["CPU(s)"] = isNaN(Number(historyCpu.to)) ? historyCpu.to : Number(historyCpu.to);
        }
      }
      const res = await fetch(
        `${API_URL}/draft-records/${encodeURIComponent(group.invalid_field)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }
      );
      const data = await res.json();
      if (!res.ok || data.status === "error") {
        showNotification(data?.message, "error");
        return;
      }
      showNotification("Draft record submitted successfully", "success");
      console.log("Draft record submitted successfully", payload);
      setDraftDialog(null);
      fetchData();
    } catch (error) {
      console.error("Submit draft failed:", error);
      showNotification("Failed to submit draft", "error");
    } finally {
      setSubmittingDraft(false);
    }
  }, [draftDialog, execID, draftFields, fetchData, showNotification]);

  return {
    selectedSuggestions,
    editedSuggestions,
    customSuggestions,
    isAccepting,
    expandedGroups,
    acceptConfirm,
    setAcceptConfirm,
    handleSelect,
    handleSelectCustom,
    handleClearCustom,
    handleCustomMetadataFetch,
    handleEditField,
    toggleGroup,
    handleAcceptConfirm,
    // L0
    l0ConfirmDialog,
    setL0ConfirmDialog,
    submittingL0,
    openL0Confirm,
    handleL0Confirm,
    // Draft
    draftDialog,
    setDraftDialog,
    draftFields,
    loadingDraftFields,
    submittingDraft,
    draftInitialValues,
    getHistoryChangesForField,
    openDraftDialog,
    handleDraftSubmit,
  };
};