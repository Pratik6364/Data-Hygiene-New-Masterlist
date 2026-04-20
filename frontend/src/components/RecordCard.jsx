
import React from "react";

import InconsistentFieldsList from "./InconsistentFieldsList";

import { useNavigate } from "react-router-dom";

import { Card, CardContent, Typography, Divider, Stack } from "@mui/material";



const STATUS_STYLES = {
    pending: { color: "#ff9500", bg: "#ffffff", border: "#ffae00", dot: "#fdba74" },
    approved: { color: "#065f46", bg: "#ffffff", border: "#10b981", dot: "#34d399" },
    accepted: { color: "#065f46", bg: "#ffffff", border: "#10b981", dot: "#34d399" },
    rejected: { color: "#991b1b", bg: "#ffffff", border: "#ef4444", dot: "#f87171" },
    "on hold": { color: "#f1cb0b", bg: "#ffffff", border: "#f1e60b", dot: "#facc15" },
};

const RecordCard = ({ record, ageColor }) => {
    const invalidFields = record["InvalidFields"];
    const navigate = useNavigate();

    const status = record.Status?.toLowerCase();
    const statusStyle = STATUS_STYLES[status];
    const statusColor = statusStyle?.color ?? "text.primary";
    const isCompleted = status === "accepted" || status === "approved";

    const cardBg = "#ffffff";
    const cardBorder = ageColor ? ageColor.border : statusStyle?.border ?? "#475569";

    const handleClick = () => navigate(`/${record.ExecutionId}`);

    return (
        <Stack justifyContent="center" alignItems="center" sx={{ mb: 3 }}>
            <Card
                onClick={handleClick}
                sx={{
                    width: "65vw",
                    backgroundColor: cardBg,
                    border: `1.3px solid ${cardBorder}`,
                    borderLeft: `6px solid ${cardBorder}`,
                    borderRadius: "8px",
                    boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03)",
                    transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                    position: "relative",
                    "&:hover": {
                        transform: "translateY(-4px)",
                        boxShadow: `0 20px 25px -5px ${cardBorder}22, 0 10px 10px -5px ${cardBorder}11`,
                        cursor: "pointer",
                        borderColor: cardBorder,
                    },
                }}
            >
                <CardContent>
                    <Stack direction="row" gap={8} alignItems="flex-start">
                        <Stack>
                            <Typography fontWeight={600}>ExecutionID:</Typography>
                            <Typography variant="body2" color="primary" fontWeight="bold">
                                {record.ExecutionId}
                            </Typography>
                        </Stack>

                        <Stack>
                            <Typography fontWeight={600}>Benchmark Category:</Typography>
                            <Typography variant="body2" color="primary" fontWeight="bold">
                                {record.BenchmarkCategory}
                            </Typography>
                        </Stack>

                        <Stack>
                            <Typography fontWeight={600}>Benchmark Type:</Typography>
                            <Typography variant="body2" color="primary" fontWeight="bold">
                                {record.BenchmarkType}
                            </Typography>
                        </Stack>

                        <Stack>
                            <Typography fontWeight={600}>Status:</Typography>
                            <Typography variant="body2" fontWeight="bold" sx={{ color: statusColor }}>
                                  {record.Status.toLowerCase() === "rejected" ? "L0 Data" : record. Status}
                            </Typography>
                        </Stack>
                    </Stack>

                    {!isCompleted && (
                        <>
                            <Divider sx={{ my: 1 }} />
                            <InconsistentFieldsList invalidFields={invalidFields} SuggestionsCount = {record.suggestionsCount} status = {record.Status}/>
                        </>
                    )}
                </CardContent>
            </Card>
        </Stack>
    );

};



export default RecordCard;

