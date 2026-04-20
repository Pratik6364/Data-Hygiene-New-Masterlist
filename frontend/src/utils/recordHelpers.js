
/**
 * Calculates the number of days since a record's relevant date.
 */
export const getDiffDays = (record) => {
  // Priority: holdedOn (for On Hold) -> updatedOn -> createdOn
  const date = record?.holdedOn || record?.updatedOn || record?.createdOn;
  if (!date) return null;
  return (Date.now() - new Date(date).getTime()) / (1000 * 60 * 60 * 24);
};

export const AGE_COLORS = {
  green:  { bg: "#ffffff", border: "#10b981", color: "#047857" },
  yellow: { bg: "#ffffff", border: "#f59e0b", color: "#b45309" },
  red:    { bg: "#ffffff", border: "#ef4444", color: "#b91c1c" },
};

/**
 * Returns a color configuration based on the age of the record.
 */
export const getAgeColor = (record) => {
  const days = getDiffDays(record);
  if (days === null) return null;
  if (days < 3) return AGE_COLORS.green;
  if (days >= 3 && days <= 6) return AGE_COLORS.yellow;
  return AGE_COLORS.red;
};

/**
 * Checks if a record matches a given age filter string.
 * @param {Object} record 
 * @param {string} filter - "<3", "3-6", or ">6"
 */
export const matchesAgeFilter = (record, filter) => {
  if (!filter) return true;
  const days = getDiffDays(record);
  if (days === null) return false;
  
  if (filter === "<3")  return days < 3;
  if (filter === "3-6") return days >= 3 && days <= 6;
  if (filter === ">6")  return days > 6;
  return true;
};
