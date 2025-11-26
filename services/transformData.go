package services

import (
	"fmt"
	"log"
	"reflect"
	"search-event-go/models"
	"sort"
	"strconv"
	"strings"

	"github.com/elliotchance/orderedmap"
	"github.com/gofiber/fiber/v2"
)

type TransformDataService struct{}

func NewTransformDataService() *TransformDataService {
	return &TransformDataService{}
}

type SortClause struct {
	Field string `json:"field"`
	Order string `json:"order"`
}

type InvalidSortFieldError struct {
	Field string
}

func (e *InvalidSortFieldError) Error() string {
	return "Invalid sort field: " + e.Field
}

type Rankings struct {
	Global          *int
	Country         *CountryRank
	Categories      []CategoryRank
	CategoryCountry []CategoryCountryRank
}

type CountryRank struct {
	ID   string
	Rank int
}

type CategoryRank struct {
	ID   string
	Rank int
}

type CategoryCountryRank struct {
	CategoryID string
	CountryID  string
	Rank       int
}

type RankingType string

const (
	RankingTypeGlobal          RankingType = "global"
	RankingTypeCountry         RankingType = "country"
	RankingTypeCategory        RankingType = "category"
	RankingTypeCategoryCountry RankingType = "category_country"
)

type PrioritizedRank struct {
	Rank       int
	RankType   RankingType
	RankRange  string
	CategoryID *string
	CountryID  *string
}

func (s *TransformDataService) ParseSortFields(sort string, filterFields models.FilterDataDto) ([]SortClause, error) {
	if sort == "" && filterFields.Q == "" {
		return []SortClause{
			{
				Field: models.SortFieldMap["score"],
				Order: "asc",
			},
		}, nil
	}

	sortFields := strings.Split(sort, ",")
	var sortClauses []SortClause

	for _, field := range sortFields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		isDescending := strings.HasPrefix(field, "-")
		cleanField := strings.TrimPrefix(field, "-")

		dbField, exists := models.SortFieldMap[cleanField]
		if !exists {
			return nil, &InvalidSortFieldError{Field: cleanField}
		}

		order := "asc"
		if isDescending {
			order = "desc"
		}

		sortClauses = append(sortClauses, SortClause{
			Field: dbField,
			Order: order,
		})
	}

	return sortClauses, nil
}

func (s *TransformDataService) getPaginationURL(limit, offset int, paginationType string, c *fiber.Ctx) *string {
	baseURL := fmt.Sprintf("%s://%s%s", c.Protocol(), c.Hostname(), c.Path())

	var newOffset int
	switch paginationType {
	case "next":
		newOffset = offset + limit
	case "previous":
		newOffset = offset - limit
		if newOffset < 0 {
			return nil
		}
	default:
		return nil
	}

	queryParams := make(map[string]string)
	c.Request().URI().QueryArgs().VisitAll(func(key, value []byte) {
		queryParams[string(key)] = string(value)
	})

	delete(queryParams, "limit")
	delete(queryParams, "offset")

	var queryParts []string
	for key, value := range queryParams {
		queryParts = append(queryParts, fmt.Sprintf("%s=%s", key, value))
	}
	queryParts = append(queryParts, fmt.Sprintf("limit=%d", limit))
	queryParts = append(queryParts, fmt.Sprintf("offset=%d", newOffset))

	queryString := strings.Join(queryParts, "&")
	url := fmt.Sprintf("%s?%s", baseURL, queryString)
	return &url
}

func (s *TransformDataService) BuildClickhouseListViewResponse(eventData []map[string]interface{}, pagination models.PaginationDto, totalCount int, c *fiber.Ctx) (interface{}, error) {
	var nextURL *string
	if pagination.Offset+pagination.Limit < totalCount {
		nextURL = s.getPaginationURL(pagination.Limit, pagination.Offset, "next", c)
	}

	var previousURL *string
	if pagination.Offset > 0 {
		previousURL = s.getPaginationURL(pagination.Limit, pagination.Offset, "previous", c)
	}

	response := fiber.Map{
		"count":    totalCount,
		"next":     nextURL,
		"previous": previousURL,
		"data":     eventData,
	}
	return response, nil
}

func (s *TransformDataService) getParsedArrayValue(filterFields models.FilterDataDto, fieldName string) []string {
	v := reflect.ValueOf(filterFields)
	t := reflect.TypeOf(filterFields)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if strings.EqualFold(fieldType.Name, "Parsed"+fieldName) {
			if field.Kind() == reflect.Slice {
				slice := field.Interface().([]string)
				return slice
			}
		}
	}
	return []string{}
}

func (s *TransformDataService) parseCoordinates(lat, lon, radius, unit string) (float64, float64, float64) {
	parsedLat, _ := strconv.ParseFloat(lat, 64)
	parsedLon, _ := strconv.ParseFloat(lon, 64)
	parsedRadius, _ := strconv.ParseFloat(radius, 64)

	radiusInMeters := parsedRadius
	conversionRates := map[string]float64{
		"km": 1000,
		"mi": 1609.34,
		"ft": 0.3048,
	}
	if rate, exists := conversionRates[unit]; exists {
		radiusInMeters = parsedRadius * rate
	}

	return parsedLat, parsedLon, radiusInMeters
}

func (s *TransformDataService) transformAggregationDataToNested(flatData []map[string]interface{}, aggregationFields []string) (interface{}, error) {
	if len(flatData) == 0 {
		return map[string]interface{}{}, nil
	}

	if len(aggregationFields) == 0 {
		aggregationFields = s.detectAggregationFields(flatData[0])
	}

	if len(aggregationFields) == 0 {
		return map[string]interface{}{}, nil
	}

	return s.transformNestedQueryData(flatData, aggregationFields)
}

func (s *TransformDataService) transformNestedQueryData(flatData []map[string]interface{}, aggregationFields []string) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic recovered in transformNestedQueryData: %v", r)
		}
	}()

	if len(aggregationFields) > 4 {
		log.Printf("WARNING: Aggregation with %d fields is not supported. Maximum supported is 4 fields.", len(aggregationFields))
		return map[string]interface{}{}, nil
	}

	parentField := aggregationFields[0]
	s.sortFlatDataByCount(flatData, parentField)

	result := orderedmap.NewOrderedMap()

	for itemIndex, item := range flatData {

		parentValue, exists := item[parentField]
		if !exists || parentValue == nil {
			continue
		}

		parentValueStr := fmt.Sprintf("%v", parentValue)
		parentCount := s.getCountFromItem(item, parentField)

		if _, exists := result.Get(parentValueStr); !exists {
			parentData := orderedmap.NewOrderedMap()
			parentData.Set(fmt.Sprintf("%sCount", parentField), parentCount)
			result.Set(parentValueStr, parentData)
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Panic recovered in nested processing for item %d: %v", itemIndex, r)
				}
			}()

			switch len(aggregationFields) {
			case 1:
				return
			case 2:
				s.processLevel2Data(item, result, parentValueStr, aggregationFields)
			case 3:
				s.processLevel3Data(item, result, parentValueStr, aggregationFields)
			case 4:
				s.processLevel4Data(item, result, parentValueStr, aggregationFields)
			}
		}()
	}

	return s.convertOrderedMapToSlice(result), nil
}

func (s *TransformDataService) processLevel2Data(item map[string]interface{}, result *orderedmap.OrderedMap, parentKey string, aggregationFields []string) {
	level1Field := aggregationFields[1]
	nestedDataKey := fmt.Sprintf("%sData", level1Field)

	nestedDataArray, exists := item[nestedDataKey]
	if !exists {
		return
	}

	parentMapValue, _ := result.Get(parentKey)
	parentMap := parentMapValue.(*orderedmap.OrderedMap)

	if _, exists := parentMap.Get(level1Field); !exists {
		parentMap.Set(level1Field, orderedmap.NewOrderedMap())
	}

	level1MapValue, _ := parentMap.Get(level1Field)
	level1Map := level1MapValue.(*orderedmap.OrderedMap)

	if dataSlice, ok := nestedDataArray.([]interface{}); ok {
		sortedDataSlice := s.sortArrayDataByCount(dataSlice)

		for _, item := range sortedDataSlice {
			if itemMap, ok := item.(map[string]interface{}); ok {
				itemName, _ := itemMap["field1Name"].(string)
				itemCount := s.parseIntFromInterface(itemMap["field1Count"])

				if itemName != "" {
					itemData := orderedmap.NewOrderedMap()
					itemData.Set(fmt.Sprintf("%sCount", level1Field), itemCount)
					level1Map.Set(itemName, itemData)
				}
			} else if itemArray, ok := item.([]interface{}); ok && len(itemArray) >= 2 {
				itemName := fmt.Sprintf("%v", itemArray[0])
				itemCount := s.parseIntFromInterface(itemArray[1])
				itemData := orderedmap.NewOrderedMap()
				itemData.Set(fmt.Sprintf("%sCount", level1Field), itemCount)
				level1Map.Set(itemName, itemData)
			} else if itemStr, ok := item.(string); ok {
				parts := strings.Split(itemStr, "|")
				if len(parts) >= 2 {
					itemName := strings.TrimSpace(parts[0])
					if itemCount, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						itemData := orderedmap.NewOrderedMap()
						itemData.Set(fmt.Sprintf("%sCount", level1Field), itemCount)
						level1Map.Set(itemName, itemData)
					}
				}
			}
		}
	}
}

func (s *TransformDataService) processLevel3Data(item map[string]interface{}, result *orderedmap.OrderedMap, parentKey string, aggregationFields []string) {
	if len(aggregationFields) < 3 {
		return
	}

	level1Field := aggregationFields[1]
	level2Field := aggregationFields[2]
	level1DataKey := fmt.Sprintf("%sData", level1Field)

	level1DataArray, exists := item[level1DataKey]
	if !exists {
		return
	}

	parentMapValue, _ := result.Get(parentKey)
	parentMap := parentMapValue.(*orderedmap.OrderedMap)

	if _, exists := parentMap.Get(level1Field); !exists {
		parentMap.Set(level1Field, orderedmap.NewOrderedMap())
	}

	level1MapValue, _ := parentMap.Get(level1Field)
	level1Map := level1MapValue.(*orderedmap.OrderedMap)

	if dataSlice, ok := level1DataArray.([]interface{}); ok {
		sortedDataSlice := s.sortDataSliceByCount(dataSlice, "field1Count")

		for _, level1Item := range sortedDataSlice {
			if level1DataMap, ok := level1Item.(map[string]interface{}); ok {
				level1Name, _ := level1DataMap["field1Name"].(string)
				level1Count := s.parseIntFromInterface(level1DataMap["field1Count"])
				level2Data, _ := level1DataMap["field2Data"].([]interface{})

				if level1Name != "" {
					level1Entry := orderedmap.NewOrderedMap()
					level1Entry.Set(fmt.Sprintf("%sCount", level1Field), level1Count)

					if len(level2Data) > 0 {
						level2Map := orderedmap.NewOrderedMap()
						sortedLevel2Data := s.sortDataSliceByCount(level2Data, "count")

						for _, level2Item := range sortedLevel2Data {
							if level2ItemMap, ok := level2Item.(map[string]interface{}); ok {
								level2Name, _ := level2ItemMap["value"].(string)
								level2Count := s.parseIntFromInterface(level2ItemMap["count"])

								if level2Name != "" {
									level2Entry := orderedmap.NewOrderedMap()
									level2Entry.Set(fmt.Sprintf("%sCount", level2Field), level2Count)
									level2Map.Set(level2Name, level2Entry)
								}
							}
						}
						if level2Map.Len() > 0 {
							level1Entry.Set(level2Field, level2Map)
						}
					}

					level1Map.Set(level1Name, level1Entry)
				}
			} else {
				unwrappedData := s.unwrapNestedArrays(level1Item)
				if level1ItemMap, ok := unwrappedData.(map[string]interface{}); ok {
					s.parseNestedLevel(level1ItemMap, level1Map, level1Field, level2Field)
				}
			}
		}
	}
}

func (s *TransformDataService) processLevel4Data(item map[string]interface{}, result *orderedmap.OrderedMap, parentKey string, aggregationFields []string) {
	if len(aggregationFields) < 4 {
		return
	}

	level1Field := aggregationFields[1]
	level2Field := aggregationFields[2]
	level3Field := aggregationFields[3]
	level1DataKey := fmt.Sprintf("%sData", level1Field)

	level1DataArray, exists := item[level1DataKey]
	if !exists {
		return
	}

	parentMapValue, _ := result.Get(parentKey)
	parentMap := parentMapValue.(*orderedmap.OrderedMap)

	if _, exists := parentMap.Get(level1Field); !exists {
		parentMap.Set(level1Field, orderedmap.NewOrderedMap())
	}

	level1MapValue, _ := parentMap.Get(level1Field)
	level1Map := level1MapValue.(*orderedmap.OrderedMap)

	if dataSlice, ok := level1DataArray.([]interface{}); ok {
		for _, level1Item := range dataSlice {
			if level1DataMap, ok := level1Item.(map[string]interface{}); ok {
				level1Name, _ := level1DataMap["field1Name"].(string)
				level1Count := s.parseIntFromInterface(level1DataMap["field1Count"])
				level2Data, _ := level1DataMap["field2Data"].([]interface{})

				if level1Name != "" {
					level1Entry := orderedmap.NewOrderedMap()
					level1Entry.Set(fmt.Sprintf("%sCount", level1Field), level1Count)

					if len(level2Data) > 0 {
						level2Map := orderedmap.NewOrderedMap()
						for _, level2Item := range level2Data {
							if level2ItemMap, ok := level2Item.(map[string]interface{}); ok {
								level2Name, _ := level2ItemMap["field2Name"].(string)
								level2Count := s.parseIntFromInterface(level2ItemMap["field2Count"])
								level3Data, _ := level2ItemMap["field3Data"].([]interface{})

								if level2Name == "" {
									level2Name, _ = level2ItemMap["value"].(string)
									level2Count = s.parseIntFromInterface(level2ItemMap["count"])
								}

								if level2Name != "" {
									level2Entry := orderedmap.NewOrderedMap()
									level2Entry.Set(fmt.Sprintf("%sCount", level2Field), level2Count)

									if len(level3Data) > 0 {
										level3Map := orderedmap.NewOrderedMap()
										for _, level3Item := range level3Data {
											if level3ItemMap, ok := level3Item.(map[string]interface{}); ok {
												level3Name, _ := level3ItemMap["value"].(string)
												level3Count := s.parseIntFromInterface(level3ItemMap["count"])

												if level3Name != "" {
													level3Entry := orderedmap.NewOrderedMap()
													level3Entry.Set(fmt.Sprintf("%sCount", level3Field), level3Count)
													level3Map.Set(level3Name, level3Entry)
												}
											}
										}
										if level3Map.Len() > 0 {
											level2Entry.Set(level3Field, level3Map)
										}
									}

									level2Map.Set(level2Name, level2Entry)
								}
							}
						}
						if level2Map.Len() > 0 {
							level1Entry.Set(level2Field, level2Map)
						}
					}

					level1Map.Set(level1Name, level1Entry)
				}
			} else if level1Str, ok := level1Item.(string); ok {
				if strings.Contains(level1Str, "|||||") {
					parts := strings.Split(level1Str, "|||||")
					if len(parts) >= 3 {
						level1Name := strings.TrimSpace(parts[0])
						if level1Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
							level2DataStr := strings.TrimSpace(parts[2])
							level1Entry := orderedmap.NewOrderedMap()
							level1Entry.Set(fmt.Sprintf("%sCount", level1Field), level1Count)

							if level2DataStr != "" {
								level2Map := orderedmap.NewOrderedMap()
								level2CountKey := fmt.Sprintf("%sCount", level2Field)

								level2Items := strings.Split(level2DataStr, " ")
								for _, level2ItemStr := range level2Items {
									level2ItemStr = strings.TrimSpace(level2ItemStr)
									if level2ItemStr == "" {
										continue
									}

									if strings.Contains(level2ItemStr, "|||") {
										level2Parts := strings.Split(level2ItemStr, "|||")
										if len(level2Parts) >= 3 {
											level2Name := strings.TrimSpace(level2Parts[0])
											if level2Count, err := strconv.Atoi(strings.TrimSpace(level2Parts[1])); err == nil {
												level3DataStr := strings.TrimSpace(level2Parts[2])
												level2Entry := orderedmap.NewOrderedMap()
												level2Entry.Set(level2CountKey, level2Count)

												if level3DataStr != "" {
													level3Map := orderedmap.NewOrderedMap()
													level3CountKey := fmt.Sprintf("%sCount", level3Field)

													level3Items := strings.Fields(level3DataStr)
													for _, level3Item := range level3Items {
														if strings.Contains(level3Item, "|") {
															level3Parts := strings.Split(level3Item, "|")
															if len(level3Parts) >= 2 {
																level3Name := strings.TrimSpace(level3Parts[0])
																if level3Count, err := strconv.Atoi(strings.TrimSpace(level3Parts[1])); err == nil {
																	level3Entry := orderedmap.NewOrderedMap()
																	level3Entry.Set(level3CountKey, level3Count)
																	level3Map.Set(level3Name, level3Entry)
																}
															}
														}
													}

													if level3Map.Len() > 0 {
														level2Entry.Set(level3Field, level3Map)
													}
												}

												level2Map.Set(level2Name, level2Entry)
											}
										}
									}
								}

								if level2Map.Len() > 0 {
									level1Entry.Set(level2Field, level2Map)
								}
							}

							level1Map.Set(level1Name, level1Entry)
						}
					}
				}
			} else {
				unwrappedData := s.unwrapNestedArrays(level1Item)
				if level1ItemMap, ok := unwrappedData.(map[string]interface{}); ok {
					s.parseNestedLevel4(level1ItemMap, level1Map, level1Field, level2Field, level3Field)
				}
			}
		}
	}
}

func (s *TransformDataService) parseIntFromInterface(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 0
}

func (s *TransformDataService) unwrapNestedArrays(data interface{}) interface{} {
	current := data

	for {
		if arr, ok := current.([]interface{}); ok && len(arr) > 0 {
			current = arr[0]
		} else {
			break
		}
	}

	return current
}

func (s *TransformDataService) parseNestedLevel(itemMap map[string]interface{}, parentMap *orderedmap.OrderedMap, level1Field, level2Field string) {
	var itemName string
	var itemCount int
	var nestedData []interface{}

	level1CountKey := fmt.Sprintf("%sCount", level1Field)
	level2DataKey := fmt.Sprintf("%sData", level2Field)

	for key, value := range itemMap {
		if key == level1CountKey {
			itemCount = s.parseIntFromInterface(value)
		} else if key == level2DataKey {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key == "" {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key != "country" && !strings.HasSuffix(key, "Count") && !strings.HasSuffix(key, "Data") {
			itemName = fmt.Sprintf("%v", value)
		}
	}

	if itemName == "" {
		for key, value := range itemMap {
			if key != "country" && key != "" &&
				!strings.HasSuffix(key, "Count") &&
				!strings.HasSuffix(key, "Data") {
				itemName = fmt.Sprintf("%v", value)
				break
			}
		}
	}

	if itemName == "" {
		return
	}

	itemData := orderedmap.NewOrderedMap()
	itemData.Set(level1CountKey, itemCount)

	if len(nestedData) > 0 {
		level2Map := orderedmap.NewOrderedMap()
		level2CountKey := fmt.Sprintf("%sCount", level2Field)

		sortedNestedData := s.sortStringArrayByCount(nestedData)

		for _, level2Item := range sortedNestedData {
			if level2Str, ok := level2Item.(string); ok {
				var parts []string
				if strings.Contains(level2Str, "|") {
					parts = strings.Split(level2Str, "|")
				} else {
					parts = strings.Fields(level2Str)
				}

				if len(parts) >= 2 {
					level2Name := strings.TrimSpace(parts[0])
					if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						level2Entry := orderedmap.NewOrderedMap()
						level2Entry.Set(level2CountKey, level2Count)
						level2Map.Set(level2Name, level2Entry)
					}
				}
			} else if level2MapItem, ok := level2Item.(map[string]interface{}); ok {
				for _, v := range level2MapItem {
					if vStr, ok := v.(string); ok {
						var parts []string
						if strings.Contains(vStr, "|") {
							parts = strings.Split(vStr, "|")
						} else {
							parts = strings.Fields(vStr)
						}

						if len(parts) >= 2 {
							level2Name := strings.TrimSpace(parts[0])
							if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
								level2Entry := orderedmap.NewOrderedMap()
								level2Entry.Set(level2CountKey, level2Count)
								level2Map.Set(level2Name, level2Entry)
							}
						}
					}
				}
			}
		}

		if level2Map.Len() > 0 {
			itemData.Set(level2Field, level2Map)
		}
	}

	parentMap.Set(itemName, itemData)
}

func (s *TransformDataService) parseNestedLevel4(itemMap map[string]interface{}, parentMap *orderedmap.OrderedMap, level1Field, level2Field, level3Field string) {
	var itemName string
	var itemCount int
	var nestedData []interface{}

	level1CountKey := fmt.Sprintf("%sCount", level1Field)
	level2DataKey := fmt.Sprintf("%sData", level2Field)

	for key, value := range itemMap {
		if key == level1CountKey {
			itemCount = s.parseIntFromInterface(value)
		} else if key == level2DataKey {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key == "" {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key != "country" && !strings.HasSuffix(key, "Count") && !strings.HasSuffix(key, "Data") {
			itemName = fmt.Sprintf("%v", value)
		}
	}

	if itemName == "" {
		for key, value := range itemMap {
			if key != "country" && key != "" &&
				!strings.HasSuffix(key, "Count") &&
				!strings.HasSuffix(key, "Data") {
				itemName = fmt.Sprintf("%v", value)
				break
			}
		}
	}

	if itemName == "" {
		return
	}

	itemData := orderedmap.NewOrderedMap()
	itemData.Set(level1CountKey, itemCount)

	if len(nestedData) > 0 {
		level2Map := orderedmap.NewOrderedMap()
		level2CountKey := fmt.Sprintf("%sCount", level2Field)

		sortedNestedData := s.sortStringArrayByCount(nestedData)

		for _, level2Item := range sortedNestedData {
			if level2Str, ok := level2Item.(string); ok {
				if strings.Contains(level2Str, "|||") {
					parts := strings.Split(level2Str, "|||")
					if len(parts) >= 3 {
						level2Name := strings.TrimSpace(parts[0])
						if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
							level3DataStr := strings.TrimSpace(parts[2])
							level2Entry := orderedmap.NewOrderedMap()
							level2Entry.Set(level2CountKey, level2Count)

							if level3DataStr != "" {
								level3Map := orderedmap.NewOrderedMap()
								level3CountKey := fmt.Sprintf("%sCount", level3Field)

								level3Items := strings.Fields(level3DataStr)
								for _, level3Item := range level3Items {
									if strings.Contains(level3Item, "|") {
										level3Parts := strings.Split(level3Item, "|")
										if len(level3Parts) >= 2 {
											level3Name := strings.TrimSpace(level3Parts[0])
											if level3Count, err := strconv.Atoi(strings.TrimSpace(level3Parts[1])); err == nil {
												level3Entry := orderedmap.NewOrderedMap()
												level3Entry.Set(level3CountKey, level3Count)
												level3Map.Set(level3Name, level3Entry)
											}
										}
									}
								}

								if level3Map.Len() > 0 {
									level2Entry.Set(level3Field, level3Map)
								}
							}

							level2Map.Set(level2Name, level2Entry)
						}
					}
				} else {
					var parts []string
					if strings.Contains(level2Str, "|") {
						parts = strings.Split(level2Str, "|")
					} else {
						parts = strings.Fields(level2Str)
					}

					if len(parts) >= 2 {
						level2Name := strings.TrimSpace(parts[0])
						if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
							level2Entry := orderedmap.NewOrderedMap()
							level2Entry.Set(level2CountKey, level2Count)
							level2Map.Set(level2Name, level2Entry)
						}
					}
				}
			} else if level2MapItem, ok := level2Item.(map[string]interface{}); ok {
				for _, v := range level2MapItem {
					if vStr, ok := v.(string); ok {
						var parts []string
						if strings.Contains(vStr, "|") {
							parts = strings.Split(vStr, "|")
						} else {
							parts = strings.Fields(vStr)
						}

						if len(parts) >= 2 {
							level2Name := strings.TrimSpace(parts[0])
							if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
								level2Entry := orderedmap.NewOrderedMap()
								level2Entry.Set(level2CountKey, level2Count)
								level2Map.Set(level2Name, level2Entry)
							}
						}
					}
				}
			}
		}

		if level2Map.Len() > 0 {
			itemData.Set(level2Field, level2Map)
		}
	}

	parentMap.Set(itemName, itemData)
}

func (s *TransformDataService) convertOrderedMapToSlice(om *orderedmap.OrderedMap) []map[string]interface{} {
	var result []map[string]interface{}

	for _, key := range om.Keys() {
		value, _ := om.Get(key)
		keyStr := fmt.Sprintf("%v", key)

		item := map[string]interface{}{
			keyStr: s.convertOrderedMapToRegularMap(value),
		}
		result = append(result, item)
	}

	return result
}

func (s *TransformDataService) sortFlatDataByCount(flatData []map[string]interface{}, parentField string) {
	countKey := fmt.Sprintf("%sCount", parentField)

	sort.Slice(flatData, func(i, j int) bool {
		countI := 0
		countJ := 0

		if countValue, exists := flatData[i][countKey]; exists {
			if countInt, ok := countValue.(int); ok {
				countI = countInt
			}
		}

		if countValue, exists := flatData[j][countKey]; exists {
			if countInt, ok := countValue.(int); ok {
				countJ = countInt
			}
		}

		return countI > countJ
	})
}

func (s *TransformDataService) sortDataSliceByCount(dataSlice []interface{}, countFieldName string) []interface{} {

	sortedSlice := make([]interface{}, len(dataSlice))
	copy(sortedSlice, dataSlice)

	sort.Slice(sortedSlice, func(i, j int) bool {
		countI := 0
		countJ := 0

		if itemMap, ok := sortedSlice[i].(map[string]interface{}); ok {
			if countValue, exists := itemMap[countFieldName]; exists {
				countI = s.parseIntFromInterface(countValue)
			}
		} else if itemArray, ok := sortedSlice[i].([]interface{}); ok && len(itemArray) >= 2 {
			countI = s.parseIntFromInterface(itemArray[1])
		}

		if itemMap, ok := sortedSlice[j].(map[string]interface{}); ok {
			if countValue, exists := itemMap[countFieldName]; exists {
				countJ = s.parseIntFromInterface(countValue)
			}
		} else if itemArray, ok := sortedSlice[j].([]interface{}); ok && len(itemArray) >= 2 {
			countJ = s.parseIntFromInterface(itemArray[1])
		}

		return countI > countJ
	})

	return sortedSlice
}

func (s *TransformDataService) sortStringArrayByCount(dataSlice []interface{}) []interface{} {
	sortedSlice := make([]interface{}, len(dataSlice))
	copy(sortedSlice, dataSlice)

	sort.Slice(sortedSlice, func(i, j int) bool {
		countI := 0
		countJ := 0

		if strI, ok := sortedSlice[i].(string); ok {
			var parts []string
			if strings.Contains(strI, "|") {
				parts = strings.Split(strI, "|")
			} else {
				parts = strings.Fields(strI)
			}
			if len(parts) >= 2 {
				if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					countI = count
				}
			}
		}

		if strJ, ok := sortedSlice[j].(string); ok {
			var parts []string
			if strings.Contains(strJ, "|") {
				parts = strings.Split(strJ, "|")
			} else {
				parts = strings.Fields(strJ)
			}
			if len(parts) >= 2 {
				if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					countJ = count
				}
			}
		}

		return countI > countJ
	})

	return sortedSlice
}

func (s *TransformDataService) sortArrayDataByCount(dataSlice []interface{}) []interface{} {
	sortedSlice := make([]interface{}, len(dataSlice))
	copy(sortedSlice, dataSlice)

	sort.Slice(sortedSlice, func(i, j int) bool {
		countI := 0
		countJ := 0

		if arrayI, ok := sortedSlice[i].([]interface{}); ok && len(arrayI) >= 2 {
			countI = s.parseIntFromInterface(arrayI[1])
		}

		if arrayJ, ok := sortedSlice[j].([]interface{}); ok && len(arrayJ) >= 2 {
			countJ = s.parseIntFromInterface(arrayJ[1])
		}

		return countI > countJ
	})

	return sortedSlice
}

func (s *TransformDataService) convertOrderedMapToRegularMap(value interface{}) interface{} {
	if nestedOM, ok := value.(*orderedmap.OrderedMap); ok {
		result := make(map[string]interface{})
		var countFields []string
		var nestedFields []string

		for _, key := range nestedOM.Keys() {
			keyStr := fmt.Sprintf("%v", key)
			if strings.HasSuffix(keyStr, "Count") {
				countFields = append(countFields, keyStr)
			} else {
				nestedFields = append(nestedFields, keyStr)
			}
		}

		for _, key := range countFields {
			nestedValue, _ := nestedOM.Get(key)
			result[key] = nestedValue
		}

		for _, key := range nestedFields {
			nestedValue, _ := nestedOM.Get(key)
			if nestedNestedOM, ok := nestedValue.(*orderedmap.OrderedMap); ok {
				sortedNested := s.convertNestedOrderedMapToSortedRegularMap(nestedNestedOM)
				result[key] = sortedNested
			} else {
				result[key] = s.convertOrderedMapToRegularMap(nestedValue)
			}
		}

		return result
	}

	return value
}

func (s *TransformDataService) convertNestedOrderedMapToSortedRegularMap(om *orderedmap.OrderedMap) interface{} {
	type KeyValue struct {
		Key   string
		Value interface{}
		Count int
	}

	var keyValues []KeyValue
	for _, key := range om.Keys() {
		value, _ := om.Get(key)
		keyStr := fmt.Sprintf("%v", key)

		count := 0
		if nestedOM, ok := value.(*orderedmap.OrderedMap); ok {
			for _, nestedKey := range nestedOM.Keys() {
				nestedKeyStr := fmt.Sprintf("%v", nestedKey)
				if strings.HasSuffix(nestedKeyStr, "Count") {
					if countValue, exists := nestedOM.Get(nestedKey); exists {
						if countInt, ok := countValue.(int); ok {
							count = countInt
							break
						}
					}
				}
			}
		}

		keyValues = append(keyValues, KeyValue{
			Key:   keyStr,
			Value: value,
			Count: count,
		})
	}

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i].Count > keyValues[j].Count
	})

	orderedKeys := make([]string, len(keyValues))
	values := make(map[string]interface{})

	for i, kv := range keyValues {
		orderedKeys[i] = kv.Key
		values[kv.Key] = s.convertOrderedMapToRegularMap(kv.Value)
	}

	return OrderedJSONMap{
		Keys:   orderedKeys,
		Values: values,
	}
}

func (s *TransformDataService) detectAggregationFields(sampleItem map[string]interface{}) []string {
	possibleFields := []string{"country", "city", "month", "date", "category", "tag", "type", "status", "edition_type"}
	var detectedFields []string

	for _, field := range possibleFields {
		if _, exists := sampleItem[field]; exists {
			detectedFields = append(detectedFields, field)
		}
	}

	for _, field := range possibleFields {
		nestedDataKey := fmt.Sprintf("%sData", field)
		if _, exists := sampleItem[nestedDataKey]; exists {
			found := false
			for _, detected := range detectedFields {
				if detected == field {
					found = true
					break
				}
			}
			if !found {
				detectedFields = append(detectedFields, field)
			}
		}
	}

	fieldOrder := []string{"country", "city", "month", "date", "category", "tag", "type", "status", "edition_type"}
	for i := 0; i < len(detectedFields)-1; i++ {
		for j := i + 1; j < len(detectedFields); j++ {
			indexA := s.indexOf(fieldOrder, detectedFields[i])
			indexB := s.indexOf(fieldOrder, detectedFields[j])
			if indexA > indexB {
				detectedFields[i], detectedFields[j] = detectedFields[j], detectedFields[i]
			}
		}
	}

	return detectedFields
}

func (s *TransformDataService) indexOf(slice []string, item string) int {
	for i, v := range slice {
		if v == item {
			return i
		}
	}
	return -1
}

func (s *TransformDataService) getCountFromItem(item map[string]interface{}, field string) int {
	countKey := fmt.Sprintf("%sCount", field)
	if count, exists := item[countKey]; exists {
		return s.parseIntFromInterface(count)
	}

	if count, exists := item["count"]; exists {
		return s.parseIntFromInterface(count)
	}

	return 0
}

func (s *TransformDataService) formatCurrency(value float64) string {
	if value == 0 {
		return "$0"
	}

	str := fmt.Sprintf("%.2f", value)

	parts := strings.Split(str, ".")
	integerPart := parts[0]
	decimalPart := parts[1]

	var result strings.Builder
	for i, char := range integerPart {
		if i > 0 && (len(integerPart)-i)%3 == 0 {
			result.WriteString(",")
		}
		result.WriteRune(char)
	}

	return "$" + result.String() + "." + decimalPart
}

func (s *TransformDataService) TransformRankings(rankingsStr string, filterFields models.FilterDataDto) map[string]interface{} {
	if rankingsStr == "" {
		return nil
	}

	ranks := Rankings{
		Categories:      []CategoryRank{},
		CategoryCountry: []CategoryCountryRank{},
	}

	rankingLines := strings.Split(rankingsStr, "<line-sep>")
	for _, line := range rankingLines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "<val-sep>")
		if len(parts) != 3 {
			continue
		}

		var countryID, categoryID *string
		if parts[0] != "null" && parts[0] != "" {
			countryID = &parts[0]
		}
		if parts[1] != "null" && parts[1] != "" {
			categoryID = &parts[1]
		}

		rankVal, err := strconv.Atoi(parts[2])
		if err != nil {
			continue
		}

		if categoryID != nil && countryID != nil {
			ranks.CategoryCountry = append(ranks.CategoryCountry, CategoryCountryRank{
				CategoryID: *categoryID,
				CountryID:  *countryID,
				Rank:       rankVal,
			})
		} else if categoryID != nil && countryID == nil {
			ranks.Categories = append(ranks.Categories, CategoryRank{
				ID:   *categoryID,
				Rank: rankVal,
			})
		} else if countryID != nil && categoryID == nil {
			ranks.Country = &CountryRank{
				ID:   *countryID,
				Rank: rankVal,
			}
		} else {
			ranks.Global = &rankVal
		}
	}

	rankType := s.DetermineRankingType(filterFields)

	prioritizedRank := s.PrioritizeRankings(filterFields, ranks, rankType)
	if prioritizedRank == nil {
		return nil
	}

	result := map[string]interface{}{
		"rank":      prioritizedRank.Rank,
		"rankType":  string(prioritizedRank.RankType),
		"rankRange": prioritizedRank.RankRange,
	}
	if prioritizedRank.CategoryID != nil {
		result["categoryId"] = *prioritizedRank.CategoryID
	} else {
		result["categoryId"] = nil
	}
	if prioritizedRank.CountryID != nil {
		result["countryId"] = *prioritizedRank.CountryID
	} else {
		result["countryId"] = nil
	}

	return result
}

func (s *TransformDataService) DetermineRankingType(filterFields models.FilterDataDto) RankingType {
	hasCategories := len(filterFields.ParsedCategory) > 0
	hasCountries := len(filterFields.ParsedCountry) > 0
	hasLocationIds := len(filterFields.ParsedLocationIds) > 0

	hasLocation := hasCountries || hasLocationIds

	if hasCategories && hasLocation {
		return RankingTypeCategoryCountry
	}
	if hasLocation {
		return RankingTypeCountry
	}
	if hasCategories {
		return RankingTypeCategory
	}
	return RankingTypeGlobal
}

func (s *TransformDataService) PrioritizeRankings(filterFields models.FilterDataDto, ranks Rankings, rankType RankingType) *PrioritizedRank {
	switch rankType {
	case RankingTypeGlobal:
		if ranks.Global != nil && *ranks.Global != 0 {
			return &PrioritizedRank{
				Rank:       *ranks.Global,
				RankType:   RankingTypeGlobal,
				RankRange:  s.GetRankRange(*ranks.Global),
				CategoryID: nil,
				CountryID:  nil,
			}
		}
		return nil

	case RankingTypeCountry:
		if ranks.Country != nil {
			matchesFilter := len(filterFields.ParsedCountry) == 0 && len(filterFields.ParsedLocationIds) == 0
			if !matchesFilter {
				for _, countryID := range filterFields.ParsedCountry {
					if ranks.Country.ID == countryID {
						matchesFilter = true
						break
					}
				}
				if !matchesFilter && len(filterFields.ParsedLocationIds) > 0 {
					matchesFilter = true
				}
			}
			if matchesFilter {
				return &PrioritizedRank{
					Rank:       ranks.Country.Rank,
					RankType:   RankingTypeCountry,
					RankRange:  s.GetRankRange(ranks.Country.Rank),
					CategoryID: nil,
					CountryID:  &ranks.Country.ID,
				}
			}
		}
		return nil

	case RankingTypeCategory:
		validCategoriesRank := []CategoryRank{}
		for _, catRank := range ranks.Categories {
			for _, catID := range filterFields.ParsedCategory {
				if catRank.ID == catID {
					validCategoriesRank = append(validCategoriesRank, catRank)
					break
				}
			}
		}
		if len(validCategoriesRank) == 0 {
			return nil
		}
		if len(validCategoriesRank) == 1 {
			return &PrioritizedRank{
				Rank:       validCategoriesRank[0].Rank,
				RankType:   RankingTypeCategory,
				RankRange:  s.GetRankRange(validCategoriesRank[0].Rank),
				CategoryID: &validCategoriesRank[0].ID,
				CountryID:  nil,
			}
		}
		minRank := validCategoriesRank[0].Rank
		for _, catRank := range validCategoriesRank {
			if catRank.Rank < minRank {
				minRank = catRank.Rank
			}
		}
		for _, catRank := range validCategoriesRank {
			if catRank.Rank == minRank {
				return &PrioritizedRank{
					Rank:       catRank.Rank,
					RankType:   RankingTypeCategory,
					RankRange:  s.GetRankRange(catRank.Rank),
					CategoryID: &catRank.ID,
					CountryID:  nil,
				}
			}
		}
		return nil

	case RankingTypeCategoryCountry:
		validCategoriesCountryRank := []CategoryCountryRank{}
		for _, catCountryRank := range ranks.CategoryCountry {
			for _, catID := range filterFields.ParsedCategory {
				if catCountryRank.CategoryID == catID {
					validCategoriesCountryRank = append(validCategoriesCountryRank, catCountryRank)
					break
				}
			}
		}
		if len(validCategoriesCountryRank) == 0 {
			return nil
		}
		if len(validCategoriesCountryRank) == 1 {
			return &PrioritizedRank{
				Rank:       validCategoriesCountryRank[0].Rank,
				RankType:   RankingTypeCategoryCountry,
				RankRange:  s.GetRankRange(validCategoriesCountryRank[0].Rank),
				CategoryID: &validCategoriesCountryRank[0].CategoryID,
				CountryID:  &validCategoriesCountryRank[0].CountryID,
			}
		}
		minRank := validCategoriesCountryRank[0].Rank
		for _, catCountryRank := range validCategoriesCountryRank {
			if catCountryRank.Rank < minRank {
				minRank = catCountryRank.Rank
			}
		}
		for _, catCountryRank := range validCategoriesCountryRank {
			if catCountryRank.Rank == minRank {
				return &PrioritizedRank{
					Rank:       catCountryRank.Rank,
					RankType:   RankingTypeCategoryCountry,
					RankRange:  s.GetRankRange(catCountryRank.Rank),
					CategoryID: &catCountryRank.CategoryID,
					CountryID:  &catCountryRank.CountryID,
				}
			}
		}
		return nil

	default:
		return nil
	}
}

func (s *TransformDataService) GetRankRange(rank int) string {
	if rank <= 100 {
		return "100"
	} else if rank <= 500 {
		return "500"
	} else if rank <= 1000 {
		return "1000"
	}
	return "1000+"
}
