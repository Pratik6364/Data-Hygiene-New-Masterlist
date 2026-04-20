import React from "react";
import { Box, Stack, Radio, Tooltip } from "@mui/material";
import EditableField from "./EditableField";
 
const SuggestionRow = ({
  suggestion,
  isSelected,
  theme,
  onSelect,
  onEditField,
  sutType,
  isPending = true,
  showRadio = true,
}) => {
  const p = theme || SELECTED;
 
  // Extract and exclude score/status from visible columns
  const { score, status, ...rawSuggestionData } = suggestion;
  
  // Filter out internal fields — no deduplication, preserve all entries including duplicates
  const internalKeys = ["_id", "execution_id", "snapshot_id", "search_key"];
  const entries = Object.entries(rawSuggestionData).filter(
    ([k]) => !internalKeys.includes(k.toLowerCase())
  );

  const [primaryKey, primaryVal] = entries[0] ?? [];
  const secondaryEntries = entries.slice(1);
 
  const tooltipTitle = (isPending && score !== undefined) ? `Confidence: ${score * 100}%` : "";
 
  return (
    <Tooltip title={tooltipTitle} arrow placement="top" disableHoverListener={!tooltipTitle}>
      <Box
        onClick={isPending ? onSelect : undefined}
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 0,
          cursor: isPending ? "pointer" : "not-allowed",
          border: `1.5px solid ${isSelected ? p.accent : "#e2e8f0"}`,
          borderRadius: 2,
          overflow: "hidden",
          backgroundColor: isSelected ? p.light : "#fff",
          transition: "all 0.15s ease",
          opacity: isPending ? 1 : 0.5,
          ...(isPending && {
            "&:hover": {
              borderColor: p.accent,
              backgroundColor: isSelected ? p.light : p.light + "40",
            },
            boxShadow: isSelected ? `0 0 0 3px ${p.accent}20` : "none",
          }),
        }}
      >
        {/* Left — Radio */}
        {showRadio && (
          <Box
            sx={{
              width: 44,
              alignSelf: "stretch",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              flexShrink: 0,
              backgroundColor: isSelected ? p.light : "#f1f5f9",
              borderRight: `1.5px solid ${isSelected ? p.accent : "#e2e8f0"}`,
              borderLeft: `3px solid ${isSelected ? p.accent : "transparent"}`,
              transition: "background-color 0.15s ease",
            }}
          >
            <Radio
              checked={isSelected}
              disabled={!isPending}
              size="small"
              sx={{
                color: "#cbd5e1",
                "&.Mui-checked": { color: p.accent },
                p: 0.5,
              }}
            />
          </Box>
        )}
 
        {/* Primary value with EditableField */}
        <Box sx={{ minWidth: 140, flexShrink: 0 }}>
          <EditableField
            label={primaryKey}
            value={primaryVal}
            color={isSelected ? p.text : "#0f172a"}
            isEditable={
              isSelected &&
              isPending &&
              primaryKey?.toLowerCase() === "cpu(s)" &&
              sutType === "vm"
            }
            onSave={(newVal) => onEditField(primaryKey, newVal)}
          />
        </Box>
 
        {/* Vertical divider */}
        {secondaryEntries.length > 0 && (
          <Box
            sx={{
              width: "1px",
              alignSelf: "stretch",
              backgroundColor: isSelected ? p.border : "#e2e8f0",
              flexShrink: 0,
            }}
          />
        )}
 
        {/* Secondary fields */}
        <Stack
          direction="row"
          alignItems="center"
          divider={
            <Box
              sx={{
                width: "1px",
                alignSelf: "stretch",
                backgroundColor: isSelected ? p.border : "#f1f5f9",
                flexShrink: 0,
              }}
            />
          }
          sx={{ flex: 1, flexWrap: "wrap" }}
        >
          {secondaryEntries.map(([key, val], i) => (
            <Box key={i} sx={{ minWidth: 100 }}>
              <EditableField
                label={key}
                value={val}
                color={isSelected ? p.text : "#334155"}
                isEditable={
                  isSelected &&
                  isPending &&
                  key.toLowerCase() === "cpu(s)" &&
                  sutType === "vm"
                }
                onSave={(newVal) => onEditField(key, newVal)}
              />
            </Box>
          ))}
        </Stack>
      </Box>
    </Tooltip>
  );
};
 
export default SuggestionRow;