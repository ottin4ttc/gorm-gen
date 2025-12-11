package model

import "gorm.io/gorm"

// Index table index info
type Index struct {
	gorm.Index
	Priority int32 `gorm:"column:SEQ_IN_INDEX"`
}

// GroupByColumn group columns
func GroupByColumn(indexList []gorm.Index) map[string][]*Index {
	columnIndexMap := make(map[string][]*Index, len(indexList))
	if len(indexList) == 0 {
		return columnIndexMap
	}

	for _, idx := range indexList {
		if idx == nil {
			continue
		}
		for i, col := range idx.Columns() {
			columnIndexMap[col] = append(columnIndexMap[col], &Index{
				Index:    idx,
				Priority: int32(i + 1),
			})
		}
	}
	return columnIndexMap
}

// GroupByColumnWithSequences group columns with correct sequences from database metadata
// indexColumnSeq: map[indexName]map[columnName]sequence (1-based)
func GroupByColumnWithSequences(indexList []gorm.Index, indexColumnSeq map[string]map[string]int32) map[string][]*Index {
	columnIndexMap := make(map[string][]*Index, len(indexList))
	if len(indexList) == 0 {
		return columnIndexMap
	}

	for _, idx := range indexList {
		if idx == nil {
			continue
		}
		indexName := idx.Name()
		columnSeqMap, hasSeq := indexColumnSeq[indexName]

		for _, col := range idx.Columns() {
			var priority int32
			if hasSeq {
				// Use sequence from database metadata if available
				if seq, ok := columnSeqMap[col]; ok {
					priority = seq
				} else {
					// Fallback: use position in Columns() array if column not found in metadata
					// This shouldn't happen, but provides a safety net
					for i, c := range idx.Columns() {
						if c == col {
							priority = int32(i + 1)
							break
						}
					}
				}
			} else {
				// Fallback to original behavior if no sequence data available
				for i, c := range idx.Columns() {
					if c == col {
						priority = int32(i + 1)
						break
					}
				}
			}

			columnIndexMap[col] = append(columnIndexMap[col], &Index{
				Index:    idx,
				Priority: priority,
			})
		}
	}
	return columnIndexMap
}
