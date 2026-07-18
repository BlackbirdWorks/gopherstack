package cloudwatch

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// matchesHistoryFilters returns true if the item passes all the given history filters.
func matchesHistoryFilters(
	item AlarmHistoryItem,
	alarmType, historyItemType string,
	startDate, endDate time.Time,
) bool {
	if alarmType != "" && item.AlarmType != alarmType {
		return false
	}
	if historyItemType != "" && item.HistoryItemType != historyItemType {
		return false
	}
	if !startDate.IsZero() && item.Timestamp.Before(startDate) {
		return false
	}
	if !endDate.IsZero() && item.Timestamp.After(endDate) {
		return false
	}

	return true
}

// DescribeAlarmHistory returns history items for one or all alarms, filtered by type and date range.
// alarmType filters by "MetricAlarm" or "CompositeAlarm" (stored on history items); empty means all.
func (b *InMemoryBackend) DescribeAlarmHistory(
	alarmName, alarmType, historyItemType, nextToken string,
	startDate, endDate time.Time,
	maxRecords int,
) (page.Page[AlarmHistoryItem], error) {
	b.mu.RLock("DescribeAlarmHistory")
	defer b.mu.RUnlock()

	var result []AlarmHistoryItem
	for name, items := range b.alarmHistory {
		if alarmName != "" && name != alarmName {
			continue
		}
		for _, item := range items {
			if matchesHistoryFilters(item, alarmType, historyItemType, startDate, endDate) {
				result = append(result, item)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})

	return page.New(result, nextToken, maxRecords, cwDefaultAlarmHistoryLimit), nil
}

// appendHistory adds a history item. Caller must hold b.mu (write lock).
// alarmTypeName should be "MetricAlarm" or "CompositeAlarm" to populate the AlarmType field.
func (b *InMemoryBackend) appendHistory(alarmName, alarmTypeName, itemType, summary, data string) {
	item := AlarmHistoryItem{
		Timestamp:       time.Now(),
		AlarmName:       alarmName,
		AlarmType:       alarmTypeName,
		HistoryItemType: itemType,
		HistorySummary:  summary,
		HistoryData:     data,
	}
	b.alarmHistory[alarmName] = append(b.alarmHistory[alarmName], item)
	// Cap history to avoid unbounded growth.
	if h := b.alarmHistory[alarmName]; len(h) > cwMaxAlarmHistory {
		b.alarmHistory[alarmName] = h[len(h)-cwMaxAlarmHistory:]
	}
}

// stateChangeHistoryData builds a JSON string for a state-change history item.
func (b *InMemoryBackend) stateChangeHistoryData(
	alarmName, oldState, newState, reason string,
) string {
	data := map[string]string{
		keyAlarmName:     alarmName,
		"OldStateValue":  oldState,
		"NewStateValue":  newState,
		"NewStateReason": reason,
	}
	// map[string]string marshaling cannot fail; error is intentionally ignored.
	bs, _ := json.Marshal(data)

	return string(bs)
}
