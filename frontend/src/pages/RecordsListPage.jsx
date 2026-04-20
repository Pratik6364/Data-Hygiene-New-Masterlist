import React, { useState, useMemo } from "react";
import { Box, Typography } from "@mui/material";
import ListHeader from "../components/ListHeader";
import RecordList from "../components/RecordList";
import ErrorPage from "../components/ErrorPage";
import { usePaginatedRecords } from "../hooks/usePaginatedRecords";
import { getAgeColor } from "../utils/recordHelpers";

/**
 * Modes and their default parameters for usePaginatedRecords
 */
const MODE_CONFIG = {
  landing: { title: "Data Hygiene Dashboard", showStatusFilters: true },
  active: { title: "My Active List", defaultStatus: "pending", showAgeFilters: true },
  completed: { title: "My Completed List", defaultStatus: "accepted,rejected", showStatusFilters: true, allowedFilters: ["accepted", "rejected"] },
  onhold: { title: "On Hold Records", defaultStatus: "On Hold", showAgeFilters: true },
  all: { title: "All Records", showStatusFilters: true },
};

// Map age filter button values → API age param values (module-level constant)
const AGE_TO_SERVER = { "<3": "green", "3-6": "yellow", ">6": "red" };

const RecordsListPage = ({ mode = "landing" }) => {
  const config = MODE_CONFIG[mode] || MODE_CONFIG.landing;

  // Age filter (client-side) or Status filter (server-side)
  const [filter, setFilter] = useState("");

  const handleFilterChange = (value) =>
    setFilter((prev) => (prev === value ? "" : value));


  // Determine which parameters to send to the API based on mode and filter
  const extraParams = useMemo(() => {
    // Age-based filter modes (active, onhold) — status always fixed; age sent when selected
    if (!config.showStatusFilters) {
      const params = { status: config.defaultStatus };
      if (filter) params.age = AGE_TO_SERVER[filter];
      return params;
    }
    // Status-based filter modes — a clicked filter always wins
    if (filter) return { status: filter };
    
    // Single-status default
    if (config.defaultStatus) return { status: config.defaultStatus };

    return {};
  }, [mode, filter, config.showStatusFilters, config.defaultStatus]);

  const {
    records,
    totalRecords,
    totalPages,
    page,
    loading,
    error,
    searchInput,
    setSearchInput,
    search,
    loadMore,
    retry,
    meta,
  } = usePaginatedRecords({ extraParams });

  // Server now handles age filtering — no client-side filtering needed
  const displayRecords = records;

  // countLabel: server always returns the correct filtered total
  const countLabel = String(totalRecords);


  // UI state for search/loading
  const isSearching = loading && searchInput !== search;

  if (error) {
    return <ErrorPage message={error?.message || "Something went wrong"} onRetry={retry} />;
  }

  return (
    <Box>
      <Box sx={{ mt: 15 }}>
        <ListHeader
          title={config.title}
          search={searchInput}
          onSearchChange={setSearchInput}
          filter={filter}
          onFilterChange={handleFilterChange}
          counts={meta}
          showAgeFilters={config.showAgeFilters}
          showStatusFilters={config.showStatusFilters}
          allowedFilters={config.allowedFilters}
          loading={isSearching}
        />

        <RecordList
          records={displayRecords}
          totalRecords={totalRecords}
          countLabel={countLabel}
          totalPages={totalPages}
          page={page}
          loading={loading}
          onLoadMore={loadMore}
          showAgeColors={config.showAgeFilters}
          showCount={config.showStatusFilters || config.showAgeFilters}
          ageColorFn={getAgeColor}
        />

        {/* Empty States */}
        {!loading && displayRecords.length === 0 && (
          <Box sx={{ textAlign: "center", mt: 4, mb: 8 }}>
            <Typography variant="h6" color="text.secondary">
              {filter ? "No records match the selected filter." : "No records found in this category."}
            </Typography>
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default RecordsListPage;
