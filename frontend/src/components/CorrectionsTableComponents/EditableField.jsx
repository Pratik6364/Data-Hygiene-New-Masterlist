import React, { useState } from "react";
import { Box, Typography, InputBase, IconButton } from "@mui/material";
import CheckOutlinedIcon from "@mui/icons-material/CheckOutlined";
import CloseOutlinedIcon from "@mui/icons-material/CloseOutlined";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";

const EditableField = ({ label, value, color, onSave, isEditable = true }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [tempValue, setTempValue] = useState(value || "");

  const handleSave = (e) => {
    e?.stopPropagation();
    onSave(tempValue);
    setIsEditing(false);
  };
  
  const handleCancel = (e) => {
    e?.stopPropagation();
    setTempValue(value || "");
    setIsEditing(false);
  };

  return (
    <Box sx={{ px: 1.5, py: 0.5, display: "flex", flexDirection: "column", minHeight: 48, minWidth: 120 }}>
      {isEditing ? (
        <>
          <Typography sx={{ fontSize: 10, fontWeight: 600, color, letterSpacing: 0.5, mb: 0.25, opacity: 0.7 }}>
            {label}
          </Typography>
          <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
            <InputBase
              autoFocus
              size="small"
              value={tempValue}
              onClick={(e) => e.stopPropagation()}
              onChange={(e) => setTempValue(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleSave(e);
                if (e.key === "Escape") handleCancel(e);
              }}
              sx={{ fontSize: 14, fontWeight: 700, color, borderBottom: `1px solid ${color}`, pb: 0.25, flex: 1 }}
            />
            <IconButton size="small" onClick={handleSave} sx={{ p: 0.25, color }}>
              <CheckOutlinedIcon sx={{ fontSize: 18 }} />
            </IconButton>
            <IconButton size="small" onClick={handleCancel} sx={{ p: 0.25, color }}>
              <CloseOutlinedIcon sx={{ fontSize: 18 }} />
            </IconButton>
          </Box>
        </>
      ) : (
        <>
          <Typography sx={{ fontSize: 10, fontWeight: 600, color, letterSpacing: 0.5, mb: 0, opacity: 0.7 }}>
            {label}
          </Typography>
          <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
            <Typography sx={{ fontSize: 14, fontWeight: 700, color }}>
              {value || "—"}
            </Typography>
            {isEditable && (
              <IconButton
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  setTempValue(value || "");
                  setIsEditing(true);
                }}
                sx={{ p: 0.25, color, opacity: 0.5, "&:hover": { opacity: 1 } }}
              >
                <EditOutlinedIcon sx={{ fontSize: 14 }} />
              </IconButton>
            )}
          </Box>
        </>
      )}
    </Box>
  );
};

export default EditableField;