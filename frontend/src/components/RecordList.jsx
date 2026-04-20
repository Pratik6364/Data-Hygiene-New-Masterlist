import React from "react";
import { Box, Typography } from "@mui/material";
import RecordCard from "./RecordCard";
import Loader from "./Loader";
import {
  List,
  InfiniteLoader,
  WindowScroller,
  AutoSizer,
} from "react-virtualized";
import "react-virtualized/styles.css";

const RecordList = ({
  records,
  totalRecords,
  countLabel,       // optional formatted string, e.g. "779+" for age-filtered views
  totalPages,
  page,
  loading,
  onLoadMore,
  showCount,
  showAgeColors,
  ageColorFn,
}) => {
  const isRowLoaded = ({ index }) => !!records[index];

  const loadMoreRows = () => {
    if (!loading && page < totalPages) {
      onLoadMore();
    }
    return Promise.resolve();
  };

  const getRowHeight = ({ index }) => {
    const record = records[index];

    if (!record) return 180;

    const status = record?.Status?.toLowerCase();
    const isCompleted = status === "accepted" || status === "approved";

    // completed records were too short and had too much empty space
    // non-completed records were slightly too tall visually
    return isCompleted ? 100 : 160;
  };

  const rowRenderer = ({ index, key, style }) => {
    const record = records[index];

    if (!record) {
      return (
        <div key={key} style={style}>
          <Loader />
        </div>
      );
    }

    return (
      <div key={key} style={style}>
        <Box sx={{ px: 2, py: 0.75 }}>
          <RecordCard
            record={record}
            ageColor={showAgeColors && ageColorFn ? ageColorFn(record) : null}
          />
        </Box>
      </div>
    );
  };

  if (loading && records.length === 0) {
    return (
      <Box sx={{ textAlign: "center", py: 4 }}>
        <Loader />
      </Box>
    );
  }

  return (
    <Box>
      {showCount ? (
        <Typography align="center" sx={{ mb: 2 }}>
          Total records: {countLabel ?? totalRecords}
        </Typography>
      ) : null}

      <InfiniteLoader
        isRowLoaded={isRowLoaded}
        loadMoreRows={loadMoreRows}
        rowCount={totalRecords}
        threshold={5}
      >
        {({ onRowsRendered, registerChild }) => (
          <WindowScroller>
            {({ height, isScrolling, scrollTop }) => (
              <AutoSizer disableHeight>
                {({ width }) => (
                  <List
                    autoHeight
                    height={height}
                    width={width}
                    scrollTop={scrollTop}
                    isScrolling={isScrolling}
                    rowCount={records.length}
                    rowHeight={getRowHeight}
                    onRowsRendered={onRowsRendered}
                    ref={registerChild}
                    rowRenderer={rowRenderer}
                    overscanRowCount={4}
                  />
                )}
              </AutoSizer>
            )}
          </WindowScroller>
        )}
      </InfiniteLoader>

      {loading && records.length > 0 && (
        <Box sx={{ textAlign: "center", py: 2 }}>
          <Loader />
        </Box>
      )}
    </Box>
  );
};

export default RecordList;