package services

import (
	"fmt"
	"regexp"
	"search-event-go/models"
	"strings"
)

type ShowValuesProcessor struct {
	showValues         string
	showValuesList     []string
	requestedAPIFields []string
	requestedFieldsSet map[string]bool
	requestedGroupsSet map[ResponseGroups]bool
	isGroupedStructure bool
}

func NewShowValuesProcessor(showValues string) *ShowValuesProcessor {
	processor := &ShowValuesProcessor{
		showValues:         showValues,
		requestedFieldsSet: make(map[string]bool),
		requestedGroupsSet: make(map[ResponseGroups]bool),
	}

	if showValues != "" {
		processor.parseShowValues()
	}

	return processor
}

func (p *ShowValuesProcessor) parseShowValues() {
	p.showValuesList = strings.Split(p.showValues, ",")
	for i := range p.showValuesList {
		p.showValuesList[i] = strings.TrimSpace(p.showValuesList[i])
	}

	p.requestedAPIFields = ResolveShowValuesToFields(p.showValuesList)

	for _, field := range p.requestedAPIFields {
		p.requestedFieldsSet[field] = true
	}

	for _, value := range p.showValuesList {
		if group, exists := ResponseGroupsMap[ResponseGroups(value)]; exists && len(group) > 0 {
			p.requestedGroupsSet[ResponseGroups(value)] = true
		}
	}

	p.isGroupedStructure = len(p.requestedGroupsSet) > 0
}

func (p *ShowValuesProcessor) ShouldIncludeField(fieldName string) bool {
	return len(p.requestedFieldsSet) == 0 || p.requestedFieldsSet[fieldName]
}

func (p *ShowValuesProcessor) IsGroupedStructure() bool {
	return p.isGroupedStructure
}

func (p *ShowValuesProcessor) GetRequestedFields() []string {
	return p.requestedAPIFields
}

func (p *ShowValuesProcessor) GetRequestedFieldsSet() map[string]bool {
	return p.requestedFieldsSet
}

func (p *ShowValuesProcessor) GetRequestedGroups() map[ResponseGroups]bool {
	return p.requestedGroupsSet
}

func (p *ShowValuesProcessor) GetShowValues() string {
	return p.showValues
}

type FieldGrouper struct {
	processor     *ShowValuesProcessor
	groupedFields map[ResponseGroups]map[string]interface{}
	flatEvent     map[string]interface{}
}

func NewFieldGrouper(processor *ShowValuesProcessor) *FieldGrouper {
	grouper := &FieldGrouper{
		processor: processor,
	}

	if processor.IsGroupedStructure() {
		grouper.groupedFields = make(map[ResponseGroups]map[string]interface{})
		for group := range processor.GetRequestedGroups() {
			grouper.groupedFields[group] = make(map[string]interface{})
		}
	} else {
		grouper.flatEvent = make(map[string]interface{})
	}

	return grouper
}

func (g *FieldGrouper) AddField(fieldName string, value interface{}) {
	if !g.processor.ShouldIncludeField(fieldName) {
		return
	}

	if g.processor.IsGroupedStructure() {
		g.addToGroup(fieldName, value)
	} else {
		g.flatEvent[fieldName] = value
	}
}

func (g *FieldGrouper) addToGroup(fieldName string, value interface{}) {
	fieldGroup := GetResponseGroupForField(fieldName)
	if fieldGroup != "" && g.groupedFields[fieldGroup] != nil {
		g.groupedFields[fieldGroup][fieldName] = value
	} else if g.groupedFields[ResponseGroupBasic] != nil {
		g.groupedFields[ResponseGroupBasic][fieldName] = value
	}
}

func (g *FieldGrouper) GetFinalEvent() map[string]interface{} {
	if g.processor.IsGroupedStructure() {
		result := make(map[string]interface{})
		for group, fields := range g.groupedFields {
			if len(fields) > 0 {
				result[string(group)] = fields
			}
		}
		return result
	}
	return g.flatEvent
}

func (g *FieldGrouper) ProcessEventFields(event map[string]interface{}) {
	for k, v := range event {
		if k == "event_id" || k == "edition_id" {
			continue
		}

		apiFieldName := g.mapDBColumnToAPIField(k)
		if g.shouldSkipField(apiFieldName) {
			continue
		}

		g.AddField(apiFieldName, v)
	}
}

func (g *FieldGrouper) mapDBColumnToAPIField(dbColumn string) string {
	if mapped, exists := DBColumnToAPIField[dbColumn]; exists {
		return mapped
	}
	return dbColumn
}

func (g *FieldGrouper) shouldSkipField(apiFieldName string) bool {
	if apiFieldName == "updated" {
		requestedFieldsSet := g.processor.GetRequestedFieldsSet()
		if len(requestedFieldsSet) > 0 &&
			!requestedFieldsSet["updated"] &&
			!requestedFieldsSet["isNew"] {
			return true
		}
	}
	return false
}

func (g *FieldGrouper) GetGroupedFields() map[ResponseGroups]map[string]interface{} {
	return g.groupedFields
}

func (g *FieldGrouper) GetFlatEvent() map[string]interface{} {
	return g.flatEvent
}

func (g *FieldGrouper) AddNestedField(fieldName string, value interface{}) {
	if !g.processor.ShouldIncludeField(fieldName) {
		return
	}

	if g.processor.IsGroupedStructure() {
		g.addToGroup(fieldName, value)
	} else {
		g.flatEvent[fieldName] = value
	}
}

func (g *FieldGrouper) AddAudienceData(countrySpreadData []map[string]interface{}, designationSpreadData []map[string]interface{}, audienceZone interface{}) {
	requestedFieldsSet := g.processor.GetRequestedFieldsSet()
	requestedGroupsSet := g.processor.GetRequestedGroups()

	var shouldIncludeSpread, shouldIncludeDesignationSpread, shouldIncludeZone bool
	if requestedGroupsSet[ResponseGroupAudience] {
		shouldIncludeSpread = true
		shouldIncludeDesignationSpread = true
		shouldIncludeZone = true
	} else {
		shouldIncludeSpread = len(requestedFieldsSet) == 0 || requestedFieldsSet["audienceSpread"]
		shouldIncludeDesignationSpread = len(requestedFieldsSet) == 0 || requestedFieldsSet["designationSpread"]
		shouldIncludeZone = len(requestedFieldsSet) == 0 || requestedFieldsSet["audienceZone"]
	}

	if !shouldIncludeSpread && !shouldIncludeDesignationSpread && !shouldIncludeZone {
		return
	}

	if g.processor.IsGroupedStructure() {
		if audienceGroup, ok := g.groupedFields[ResponseGroupAudience]; ok {
			if shouldIncludeSpread {
				if len(countrySpreadData) > 0 {
					audienceGroup["countrySpread"] = countrySpreadData
				} else {
					audienceGroup["countrySpread"] = []interface{}{}
				}
			}
			if shouldIncludeDesignationSpread {
				if len(designationSpreadData) > 0 {
					audienceGroup["designationSpread"] = designationSpreadData
				} else {
					audienceGroup["designationSpread"] = []interface{}{}
				}
			}
			if shouldIncludeZone {
				audienceGroup["audienceZone"] = audienceZone
			}
		}
	} else {
		audienceData := make(map[string]interface{})
		if shouldIncludeSpread {
			if len(countrySpreadData) > 0 {
				audienceData["countrySpread"] = countrySpreadData
			} else {
				audienceData["countrySpread"] = []interface{}{}
			}
		}
		if shouldIncludeDesignationSpread {
			if len(designationSpreadData) > 0 {
				audienceData["designationSpread"] = designationSpreadData
			} else {
				audienceData["designationSpread"] = []interface{}{}
			}
		}
		if shouldIncludeZone {
			audienceData["audienceZone"] = audienceZone
		}
		if shouldIncludeSpread || shouldIncludeDesignationSpread || shouldIncludeZone {
			g.flatEvent["audience"] = audienceData
		}
	}
}

type BaseFieldsSelector struct {
	processor    *ShowValuesProcessor
	filterFields models.FilterDataDto
}

func NewBaseFieldsSelector(processor *ShowValuesProcessor, filterFields models.FilterDataDto) *BaseFieldsSelector {
	return &BaseFieldsSelector{
		processor:    processor,
		filterFields: filterFields,
	}
}

func (s *BaseFieldsSelector) GetBaseFields() []string {
	if s.processor.GetShowValues() != "" {
		return s.getCustomFields()
	}
	return s.getDefaultFields()
}

func (s *BaseFieldsSelector) getCustomFields() []string {
	requestedAPIFields := s.processor.GetRequestedFields()
	baseFields := MapAPIFieldsToDBSelect(requestedAPIFields)
	return append([]string{"ee.event_id", "ee.edition_id"}, baseFields...)
}

func (s *BaseFieldsSelector) getDefaultFields() []string {
	baseFields := []string{
		"ee.event_uuid as id",
		"ee.event_id",
		"ee.start_date as start",
		"ee.end_date as end",
		"ee.event_name as name",
		"ee.event_abbr_name as shortName",
		"ee.edition_city_name as city",
		"ee.edition_country as country",
		"ee.event_description as description",
		"ee.event_followers as followers",
		"ee.event_logo as logo",
		"ee.event_avgRating as avgRating",
		"ee.exhibitors_lower_bound as exhibitors_lower_bound",
		"ee.exhibitors_upper_bound as exhibitors_upper_bound",
		"ee.event_format as format",
		"ee.yoyGrowth as yoyGrowth",
		"ee.tickets as tickets",
	}

	if s.filterFields.EventEstimate {
		baseFields = append(baseFields, "ee.event_economic_value as economicImpact")
		baseFields = append(baseFields, "ee.event_economic_breakdown as economicImpactBreakdown")
	}

	if s.filterFields.ImpactScore {
		baseFields = append(baseFields, "ee.impactScore as impactScore")
	}

	return baseFields
}

func (s *BaseFieldsSelector) BuildBaseFieldMap(baseFields []string) map[string]bool {
	baseFieldMap := make(map[string]bool)
	for _, field := range baseFields {
		fieldName := strings.Replace(field, "ee.", "", 1)
		if strings.Contains(fieldName, " as ") {
			parts := strings.Split(fieldName, " as ")
			fieldName = strings.TrimSpace(parts[1])
		}
		baseFieldMap[fieldName] = true
	}
	return baseFieldMap
}

func (s *BaseFieldsSelector) BuildDBColumnToAliasMap(baseFields []string) map[string]string {
	dbToAlias := make(map[string]string)
	for _, field := range baseFields {
		fieldName := strings.Replace(field, "ee.", "", 1)
		if strings.Contains(fieldName, " as ") {
			parts := strings.Split(fieldName, " as ")
			dbColumn := strings.TrimSpace(parts[0])
			alias := strings.TrimSpace(parts[1])
			dbToAlias[dbColumn] = alias
			dbToAlias[alias] = alias
		} else {
			dbToAlias[fieldName] = fieldName
		}
	}
	return dbToAlias
}

func (s *BaseFieldsSelector) FixOrderByForFields(orderByClause string, selectedFields []string) string {
	if orderByClause == "" {
		return ""
	}

	dbToAliasMap := s.BuildDBColumnToAliasMap(selectedFields)

	aliasToAlias := make(map[string]string)
	for _, field := range selectedFields {
		fieldName := strings.Replace(field, "ee.", "", 1)
		if strings.Contains(fieldName, " as ") {
			parts := strings.Split(fieldName, " as ")
			alias := strings.TrimSpace(parts[1])
			aliasToAlias[alias] = alias
		}
	}

	fixedClause := orderByClause

	fixedClause = strings.TrimPrefix(fixedClause, "ORDER BY ")
	fixedClause = strings.TrimSpace(fixedClause)

	for dbColumn, alias := range dbToAliasMap {
		fixedClause = strings.ReplaceAll(fixedClause, fmt.Sprintf("ee.%s", dbColumn), alias)
		fieldPattern := fmt.Sprintf("\\b%s\\b", dbColumn)
		re := regexp.MustCompile(fieldPattern)
		fixedClause = re.ReplaceAllString(fixedClause, alias)
	}

	fixedClause = regexp.MustCompile(`\bee\.`).ReplaceAllString(fixedClause, "")

	if fixedClause != "" {
		fixedClause = "ORDER BY " + fixedClause
	}

	return fixedClause
}

type RelatedDataQueryBuilder struct {
	processor    *ShowValuesProcessor
	filterFields models.FilterDataDto
	queryResult  *ClickHouseQueryResult
	eventIdsStr  string
}

func NewRelatedDataQueryBuilder(processor *ShowValuesProcessor, filterFields models.FilterDataDto, queryResult *ClickHouseQueryResult, eventIdsStr string) *RelatedDataQueryBuilder {
	return &RelatedDataQueryBuilder{
		processor:    processor,
		filterFields: filterFields,
		queryResult:  queryResult,
		eventIdsStr:  eventIdsStr,
	}
}

func (b *RelatedDataQueryBuilder) BuildQuery() string {
	query := b.buildBaseQuery()

	if b.shouldIncludeJobComposite() {
		query += b.buildJobCompositeQuery()
	}

	if b.shouldIncludeAudienceSpread() {
		query += b.buildAudienceSpreadQuery()
	}

	if b.shouldIncludeRankings() {
		query += b.buildRankingsQuery()
	}

	return query
}

func (b *RelatedDataQueryBuilder) buildBaseQuery() string {
	return fmt.Sprintf(`
		WITH current_events AS (
    		SELECT edition_id
    		FROM testing_db.allevent_ch
    		WHERE event_id IN (%s)
			AND edition_type = 'current_edition'
		)	
		SELECT 
			event AS event_id,
			'category' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value,
			arrayStringConcat(groupArray(category_uuid), ', ') AS uuid_value,
			arrayStringConcat(groupArray(slug), ', ') AS slug_value,
			'' AS eventGroupType_value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 1
		GROUP BY event

		UNION ALL

		SELECT 
			event AS event_id,
			'tags' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value,
			arrayStringConcat(groupArray(category_uuid), ', ') AS uuid_value,
			arrayStringConcat(groupArray(slug), ', ') AS slug_value,
			'' AS eventGroupType_value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 0
		GROUP BY event

		UNION ALL

		SELECT 
			event_id,
			'types' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value,
			arrayStringConcat(groupArray(eventtype_uuid), ', ') AS uuid_value,
			arrayStringConcat(groupArray(slug), ', ') AS slug_value,
			arrayStringConcat(groupArray(eventGroupType), ', ') AS eventGroupType_value
		FROM testing_db.event_type_ch
		WHERE event_id IN (%s)
		GROUP BY event_id
	`, b.eventIdsStr, b.eventIdsStr, b.eventIdsStr, b.eventIdsStr)
}

func (b *RelatedDataQueryBuilder) shouldIncludeJobComposite() bool {
	return len(b.filterFields.ParsedJobComposite) > 0 && b.queryResult.needsDesignationJoin
}

func (b *RelatedDataQueryBuilder) buildJobCompositeQuery() string {
	escapedDesignations := make([]string, len(b.filterFields.ParsedJobComposite))
	for i, designation := range b.filterFields.ParsedJobComposite {
		escapedDesignations[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(designation, "'", "''"))
	}
	designationsStr := strings.Join(escapedDesignations, ",")

	return fmt.Sprintf(`
		UNION ALL

		SELECT 
			event_id,
			'jobComposite' AS data_type,
			concat(display_name, '|||', role, '|||', department) AS value,
			'' AS uuid_value,
			'' AS slug_value,
			'' AS eventGroupType_value
		FROM testing_db.event_designation_ch
		WHERE edition_id IN (SELECT edition_id FROM current_events) 
			AND display_name IN (%s)
			AND total_visitors >= 5
		ORDER BY event_id, total_visitors DESC
	`, designationsStr)
}

func (b *RelatedDataQueryBuilder) shouldIncludeAudienceSpread() bool {
	if len(b.filterFields.ParsedAudienceSpread) > 0 && b.queryResult.needsAudienceSpreadJoin {
		return true
	}

	requestedFieldsSet := b.processor.GetRequestedFieldsSet()
	requestedGroupsSet := b.processor.GetRequestedGroups()

	if requestedGroupsSet[ResponseGroupAudience] {
		return true
	}

	if len(requestedFieldsSet) == 0 {
		return true
	}

	return requestedFieldsSet["audienceSpread"] || requestedFieldsSet["designationSpread"]
}

func (b *RelatedDataQueryBuilder) buildAudienceSpreadQuery() string {
	limitClause := ""
	if b.shouldLimitAudienceSpread() {
		limitClause = "\n\t\tLIMIT 5 BY event_id"
	}

	query := `
		UNION ALL

		SELECT 
			event_id,
			'countrySpread' AS data_type,
			CAST(country_data as String) AS value,
			'' AS uuid_value,
			'' AS slug_value,
			'' AS eventGroupType_value
		FROM testing_db.event_visitorSpread_ch
		ARRAY JOIN user_by_cntry AS country_data
		WHERE event_id IN (` + b.eventIdsStr + `)
		ORDER BY event_id, JSONExtractInt(CAST(country_data as String), 'total_count') DESC` + limitClause + `
		`

	query += `
		UNION ALL

		SELECT 
			event_id,
			'designationSpread' AS data_type,
			CAST(designation_data as String) AS value,
			'' AS uuid_value,
			'' AS slug_value,
			'' AS eventGroupType_value
		FROM testing_db.event_visitorSpread_ch
		ARRAY JOIN user_by_designation AS designation_data
		WHERE event_id IN (` + b.eventIdsStr + `)
		ORDER BY event_id, JSONExtractInt(CAST(designation_data as String), 'total_count') DESC` + limitClause + `
		`

	return query
}

func (b *RelatedDataQueryBuilder) shouldLimitAudienceSpread() bool {
	if b.processor.GetShowValues() == "" || b.filterFields.View == "" {
		return false
	}
	viewLower := strings.ToLower(strings.TrimSpace(b.filterFields.View))
	return viewLower == "list" || viewLower == "tracker"
}

func (b *RelatedDataQueryBuilder) shouldIncludeRankings() bool {
	if b.processor.GetShowValues() == "" || b.filterFields.View == "" {
		return false
	}

	viewLower := strings.ToLower(strings.TrimSpace(b.filterFields.View))
	if viewLower != "list" && viewLower != "tracker" {
		return false
	}

	requestedGroupsSet := b.processor.GetRequestedGroups()
	requestedFieldsSet := b.processor.GetRequestedFieldsSet()

	if len(requestedGroupsSet) > 0 {
		if requestedGroupsSet[ResponseGroupInsights] {
			return true
		}
	}

	if len(requestedFieldsSet) > 0 {
		return requestedFieldsSet["rankings"]
	}

	return false
}

func (b *RelatedDataQueryBuilder) buildRankingsQuery() string {
	return `
		UNION ALL

		SELECT 
			event_id,
			'rankings' AS data_type,
			arrayStringConcat(
				groupUniqArray(
					concat(
						if(toString(country) = '', 'null', toString(country)), '<val-sep>',
						if(toString(category_name) = '', 'null', toString(category_name)), '<val-sep>',
						toString(ifNull(event_rank, 0))
					)
				),
				'<line-sep>'
			) AS value,
			'' AS uuid_value,
			'' AS slug_value,
			'' AS eventGroupType_value
		FROM testing_db.event_ranking_ch
		WHERE event_id IN (` + b.eventIdsStr + `)
		GROUP BY event_id
		`
}
