import React from "react";
import { Box, Paper, Typography } from "@mui/material";

const COLUMNS = 3;

const ExecutionInfoBox = ({ executionInfo }) => {
  const entries = Object.entries(executionInfo);

  return (
    <Box
      sx={{
        border: "1px solid rgba(169, 172, 175, 0.57)",
        background:"#f8fafc",
        borderRadius: 2,
        p: 3,
        mb: 5,
      }}
    >
      <Typography variant="h6" sx={{ fontWeight: 700, color: "#17233a", mb: 2 }}>
        Execution Information
      </Typography>

      <Paper
        elevation={0}
        sx={{
          border: "1px solid #c2c2c2a2",
          borderRadius: 2,
          bgcolor: "#f5f5f5",
          overflow: "hidden",
        }}
      >
        <Box
          sx={{
            display: "grid",
            gridTemplateColumns: { xs: "1fr", md: `repeat(${COLUMNS}, 1fr)` },
          }}
        >
          {entries.map(([key, value], index) => {
            const isLastInRow = (index + 1) % COLUMNS === 0;
            const isLastRow = index >= entries.length - (entries.length % COLUMNS || COLUMNS);

            return (
              <Box
                key={key}
                sx={{
                  p: 2,
                  borderRight: { md: isLastInRow ? "none" : "1px solid #c2c2c2a2" },
                  borderBottom: isLastRow ? "none" : "1px solid #c2c2c2a2",
                }}
              >
                <Typography sx={{ fontSize: 13, color: "#5b6b82", fontWeight: 600, mb: 0.5 }}>
                  {key}
                </Typography>
                <Typography sx={{ fontSize: 16, color: "#17233a", fontWeight: 600 }}>
                  {value ? value : "-"}
                </Typography>
              </Box>
            );
          })}
        </Box>
      </Paper>
    </Box>
  );
};

export default ExecutionInfoBox;