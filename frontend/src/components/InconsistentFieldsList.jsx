import React from "react";
import { Stack, Typography, Chip, Box } from "@mui/material";

const InconsistentFieldsList = ({ invalidFields = [], SuggestionsCount, status }) => {
  const errors = invalidFields;
  const MAX_VISIBLE = 8;
 
  const visibleErrors = errors.slice(0, MAX_VISIBLE);
  const remaining = errors.length - MAX_VISIBLE;
 
  return (
    <Box>
      <Stack spacing={0.4} mt={0.25}>
        <Typography
          sx={{
            fontSize: "0.72rem",
            fontWeight: 700,
            color: "text.secondary",
            textTransform: "uppercase",
            letterSpacing: 0.4,
            lineHeight: 1.1,
            opacity: 0.85,
          }}
        >
          Inconsistent Fields
        </Typography>
 
        <Stack flexDirection={"row"} sx={{justifyContent: "space-between"}}>
          <Stack
            direction="row"
            spacing={0.6}
            useFlexGap
            flexWrap="wrap"
            sx={{
              alignItems: "center",
            }}
          >
            {visibleErrors.map((e, i) => (
              <Chip
                key={i}
                label={e}
                size="small"
                color="error"
                variant="outlined"
                sx={{
                  height: 22,
                  fontSize: "0.7rem",
                  fontWeight: 600,
                  borderRadius: "8px",
                }}
              />
            ))}
 
            {remaining > 0 && (
              <Typography
                sx={{
                  fontSize: "0.72rem",
                  color: "text.secondary",
                  alignSelf: "center",
                }}
              >
                +{remaining} more
              </Typography>
            )}
          </Stack>
        <Typography variant="caption" sx={{fontStyle: "italic"}}>{status.toLowerCase() === "pending" && (SuggestionsCount == true ? "Suggestions available" : "")}</Typography>
        </Stack>
      </Stack>
    </Box>
  );
};
 
export default InconsistentFieldsList;