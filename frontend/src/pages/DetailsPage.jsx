import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { Box, Snackbar, Alert } from "@mui/material";
import ErrorPage from "../components/ErrorPage";
import Loader from "../components/Loader";
import ExecutionInfoBox from "../components/ExecutionInfoBox";
import CorrectionsTable from "../components/CorrectionsTable";
import { API_URL } from "../config";

const transformOldApiData = (rawData) => {
  if (!Array.isArray(rawData)) return [];

  const existing_data = rawData.map((item) => ({
    field: item.field,
    value: item.value,
    validation_status: item.validation_status,
  }));

  const nonValidFields = rawData.filter((item) => item.validation_status !== "valid");
  if (nonValidFields.length === 0) return [];

  const maxSuggestions = Math.max(
    ...nonValidFields.map((item) =>
      Array.isArray(item.comparing_data) ? item.comparing_data.length : 0
    ),
    1
  );

  const suggestions = [];
  for (let i = 0; i < maxSuggestions; i++) {
    const sugg = {};
    let firstScore = null;

    nonValidFields.forEach((item) => {
      const entry = (item.comparing_data ?? [])[i];
      const suggKey = `suggestion${i + 1}`;
      const scoreKey = `score${i + 1}`;

      sugg[item.field.toLowerCase()] =
        entry && entry[suggKey] !== undefined ? entry[suggKey] : "—";
      
      if (entry && entry[scoreKey] !== undefined && firstScore === null) {
        firstScore = entry[scoreKey];
      }
    });

    if (firstScore !== null) {
      sugg.score = firstScore;
    }
    suggestions.push(sugg);
  }

  const groupLabel = nonValidFields.map((item) => item.field).join(", ");
  return [{ invalid_field: groupLabel, existing_data, suggestions }];
};

const DetailsPage = () => {
  const { id } = useParams();

  const [error, setError] = useState(null);
  const [executionData, setExecutionData] = useState(null);
  const [data, setData] = useState(null);
  const [history, setHistory] = useState(null);
  const [loading, setLoading] = useState(true); // ← true so Loader shows on first paint
  const [snackbar, setSnackbar] = useState({ open: false, message: "", severity: "success" });

  const showNotification = (message, severity = "success") => {
    setSnackbar({ open: true, message, severity });
  };

  const handleSnackbarClose = () => setSnackbar(prev => ({ ...prev, open: false }));

  const fetchData = async () => {
    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`${API_URL}/snapshot-records/${id}`);
      if (!res.ok) throw new Error("Failed to fetch record details");

      const json = await res.json();
      setExecutionData(json.execution_details);
      setHistory(json.history);

      if (
        Array.isArray(json.data) &&
        json.data.length > 0 &&
        json.data[0]?.invalid_field !== undefined
      ) {
        setData(json.data);
      } else if (Array.isArray(json.Data)) {
        setData(transformOldApiData(json.Data));
      } else {
        setData([]);
      }
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [id]);

  if (loading) return <Loader />;
  if (error) return <ErrorPage message={error?.message} onRetry={fetchData} />;
  if (!executionData) return "No execution data to show";
  // if (!data || data.length === 0) return "No invalid data in the response to show";

  return (
    <Box>
      <Box sx={{ p: 4, mt: 5 }}>
        <ExecutionInfoBox executionInfo={executionData} />
        <Box sx={{ mt: -2 }}>
          <CorrectionsTable
            data={data}
            history={history}
            execID={executionData.execution_id}
            sutType={executionData.sutType}
            fetchData={fetchData}
            showNotification={showNotification}
          />
        </Box>
      </Box>

      <Snackbar 
        open={snackbar.open} 
        autoHideDuration={3000} 
        onClose={handleSnackbarClose}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
      >
        <Alert onClose={handleSnackbarClose} severity={snackbar.severity} variant="filled" sx={{ width: "100%" }}>
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default DetailsPage;