import React from "react";
import { Box, Typography, TextField, Stack, CircularProgress, InputAdornment } from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import Navbar from "./Navbar";

/**
 * Common styling for the status/age patches
 */
const patchSx = (isActive, activeBg, bg, activeColor, color, dot) => ({
  display: "flex",
  alignItems: "center",
  gap: 1,
  px: 2.2,
  py: 1,
  borderRadius: "8px",
  cursor: "pointer",
  userSelect: "none",
  background: isActive ? activeBg : "#ffffff",
  border: `1.3px solid ${isActive ? activeBg : "#727579ff"}`,
  color: isActive ? "#fff" : "#475569",
  fontWeight: isActive ? 600 : 500,
  fontSize: "0.85rem",
  transition: "all 0.2s ease",
  boxShadow: isActive
    ? `0 4px 12px ${activeBg}44`
    : "0 1px 2px rgba(0,0,0,0.05)",
  "&:hover": {
    borderColor: isActive ? activeBg : color,
    backgroundColor: isActive ? activeBg : "#f8fafc",
    transform: "translateY(-1px)",
    boxShadow: isActive ? `0 6px 15px ${activeBg}66` : "0 4px 6px rgba(0,0,0,0.05)",
  },
});

const dotSx = (isActive, dot) => ({
  width: 8,
  height: 8,
  borderRadius: "50%",
  background: isActive ? "#fff" : dot,
  flexShrink: 0,
  transition: "background 0.2s ease",
});

const ListHeader = ({
  title,
  search,
  onSearchChange,
  filter,
  onFilterChange,
  counts = {},
  showAgeFilters = false,
  showStatusFilters = false,
  allowedFilters,   // optional array of status values to restrict which buttons show
  loading = false,
}) => {
  const AGE_FILTERS = [
    {
      label: `< 3 Days`,
      value: "<3",
      countKey: "green",
      color: "#059669",
      activeColor: "#065f46",
      bg: "#ffffff",
      activeBg: "#10b981",
      dot: "#34d399",
    },
    {
      label: `3 - 6 Days`,
      value: "3-6",
      countKey: "yellow",
      color: "#d97706",
      activeColor: "#b45309",
      bg: "#ffffff",
      activeBg: "#f59e0b",
      dot: "#fbbf24",
    },
    {
      label: `> 6 Days`,
      value: ">6",
      countKey: "red",
      color: "#dc2626",
      activeColor: "#991b1b",
      bg: "#ffffff",
      activeBg: "#ef4444",
      dot: "#f87171",
    },
  ];

  const STATUS_FILTERS = [
    { label: "Pending",  value: "pending", color: "#ea580c", activeColor: "#9a3412", bg: "#ffffff", activeBg: "#f97316", dot: "#f9972fff" },
    { label: "Accepted", value: "accepted", color: "#059669", activeColor: "#065f46", bg: "#ffffff", activeBg: "#10b981", dot: "#34d399" },
    { label: "L0 Data", value: "rejected", color: "#dc2626", activeColor: "#991b1b", bg: "#ffffff", activeBg: "#ef4444", dot: "#f87171" },
    { label: "On Hold",  value: "On Hold",  color: "#ca8a04", activeColor: "#854d0e", bg: "#ffffff", activeBg: "#dbbc23", dot: "#facc15" },
  ];

  return (
    <Box>
      <Navbar />
      <Typography variant="h3" align="center" sx={{ my: 3, mt: -4, fontWeight: 700, color: "#1e293b" }}>
        {title}
      </Typography>

      <Stack
        direction="column"
        alignItems="center"
        justifyContent="center"
        gap={4}
        sx={{ px: 2, mb: 3, flexWrap: "wrap" }}
      >
        <TextField
          placeholder="Search Execution ID, Type, or Category..."
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          variant="outlined"
          sx={{
            width: { xs: "90%", md: 500 },
            "& .MuiOutlinedInput-root": {
              border: "1px solid #747d8aff",
              borderRadius: "8px",
              backgroundColor: "#fff",
              boxShadow: "0 2px 10px rgba(0,0,0,0.05)",
              "&:hover": { boxShadow: "0 4px 15px rgba(0,0,0,0.1)" },
            }
          }}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon sx={{ color: "#94a3b8" }} />
                </InputAdornment>
              ),
              endAdornment: loading ? (
                <InputAdornment position="end">
                  <CircularProgress size={18} thickness={5} />
                </InputAdornment>
              ) : null
            }
          }}
        />

        <Stack direction="row" gap={1.5} sx={{ flexWrap: "wrap", justifyContent: "center" }}>
          {showAgeFilters && AGE_FILTERS.map((f) => {
            const isActive = filter === f.value;
            const count = counts[f.countKey];
            return (
              <Box
                key={f.value}
                onClick={() => onFilterChange(f.value)}
                sx={patchSx(isActive, f.activeBg, f.bg, f.activeColor, f.color, f.dot)}
              >
                <Box sx={dotSx(isActive, f.dot)} />
                {f.label}

              </Box>
            );
          })}


          {showStatusFilters && STATUS_FILTERS
            .filter((f) => !allowedFilters || allowedFilters.includes(f.value))
            .map((f) => {
              const isActive = filter === f.value;
              return (
                <Box
                  key={f.value}
                  onClick={() => onFilterChange(f.value)}
                  sx={patchSx(isActive, f.activeBg, f.bg, f.activeColor, f.color, f.dot)}
                >
                  <Box sx={dotSx(isActive, f.dot)} />
                  {f.label}
                </Box>
              );
            })}
        </Stack>
      </Stack>
    </Box>
  );
};

export default ListHeader;
