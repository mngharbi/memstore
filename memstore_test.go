package memstore

import (
	"math/rand"
	"reflect"
	"testing"
)

type TestStruct struct {
	id         int
	importance float32
	name       string
}

// Comparator for multiple keys for values
func (ts TestStruct) Less(index string, than interface{}) bool {
	switch index {
	case "id":
		return ts.id < than.(TestStruct).id
	case "importance":
		return ts.importance < than.(TestStruct).importance
	case "name":
		return ts.name < than.(TestStruct).name
	default:
		return true
	}
}

func testData() []TestStruct {
	return []TestStruct{
		TestStruct{1, 3, "x"},
		TestStruct{2, 2, "y"},
		TestStruct{3, 5, "z"},
		TestStruct{4, 0, "t"},
		TestStruct{8, 3.2, "u"},
		TestStruct{9, 3.1, "v"},
	}
}

func idSortedData() []TestStruct {
	return testData()
}

func importanceSortedData() []TestStruct {
	return []TestStruct{
		TestStruct{4, 0, "t"},
		TestStruct{2, 2, "y"},
		TestStruct{1, 3, "x"},
		TestStruct{9, 3.1, "v"},
		TestStruct{8, 3.2, "u"},
		TestStruct{3, 5, "z"},
	}
}

func shuffeledTestData() (data []TestStruct) {
	data = testData()

	for i := range data {
		j := rand.Intn(i + 1)
		data[i], data[j] = data[j], data[i]
	}

	return data
}

/*
	Adding
*/

func TestAddOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	if ms.Len() != len(data) {
		t.Error("Adding with one index failed")
	}
}

func TestAddMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"importance", "id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	if ms.Len() != len(data) {
		t.Error("Adding with multiple index failed")
	}
}

/*
	Add or Get
*/

func TestAddOrGetOneIndex(t *testing.T) {
	data := shuffeledTestData()

	// AddOrGet all unique test data
	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.AddOrGet(vItem)
	}

	// AddOrGet modified first record
	firstItemPristine := data[0]
	firstItem := data[0]
	firstItem.importance += 1
	var vItem Item = firstItem
	resultItem := ms.AddOrGet(vItem)

	if ms.Len() != len(data) ||
		!reflect.DeepEqual(firstItemPristine, resultItem.(TestStruct)) {
		t.Error("AddOrGetting with one index failed. expected=%+v found=%+v", firstItemPristine, resultItem.(TestStruct))
	}
}

/*
	Delete
*/

func TestDeleteInvalidIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 3}
	result := ms.Delete(searchedRecord, "notID")

	if result != nil {
		t.Error("Deleting with unspecified index didn't fail")
	}
}

// One index, Success
func TestDeleteOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 3}
	result := ms.Delete(searchedRecord, "id").(TestStruct)

	if ms.Len() != len(data)-1 ||
		result.id != 3 || result.importance != 5 {
		t.Error("Deleting with one index failed")
	}
}

// One index, Empty
func TestDeleteEmptyOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 10}
	result := ms.Delete(searchedRecord, "id")

	if result != nil {
		t.Error("Deleting inexistent with one index failed")
	}
}

// Multiple index, Success
func TestDeleteMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{importance: 5}
	result := ms.Delete(searchedRecord, "importance").(TestStruct)

	if ms.Len() != len(data)-1 ||
		result.id != 3 || result.importance != 5 {
		t.Error("Deleting with multiple index failed")
	}
}

// Multiple index, Empty
func TestDeleteEmptyMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{importance: 6}
	result := ms.Delete(searchedRecord, "importance")

	if result != nil {
		t.Error("Deleting inexistent with multiple index failed")
	}
}

/*
	Get
*/

func TestGetInvalidIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 3}
	result := ms.Get(searchedRecord, "notID")

	if result != nil {
		t.Error("Getting with unspecified index didn't fail")
	}
}

// One index, Success
func TestGetOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 3}

	result := ms.Get(searchedRecord, "id").(TestStruct)

	if result.id != 3 || result.importance != 5 {
		t.Error("Get with one index failed")
	}
}

// One index, Empty
func TestGetEmptyOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 10}

	result := ms.Get(searchedRecord, "id")

	if result != nil {
		t.Error("Get inexistent with one index failed")
	}
}

// Multiple index, Success
func TestGetMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{importance: 5}
	result := ms.Get(searchedRecord, "importance").(TestStruct)

	if result.id != 3 || result.importance != 5 {
		t.Error("Get with multiple index failed")
	}
}

// Multiple index, Empty
func TestGetEmptyMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{importance: 6}
	result := ms.Get(searchedRecord, "importance")

	if result != nil {
		t.Error("Get inexistent with multiple index failed")
	}
}

/*
	GetRange
*/

func TestGetRangeInvalidIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = TestStruct{id: 2}, TestStruct{id: 7}

	var res []TestStruct = make([]TestStruct, 0)

	ms.GetRange(from, to, "notID", func(item Item) bool {
		res = append(res, item.(TestStruct))
		return true
	})

	if len(res) != 0 {
		t.Error("Getting range with unspecified index didn't fail")
	}
}

func TestRangeOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = TestStruct{id: 2}, TestStruct{id: 7}

	var res []TestStruct = make([]TestStruct, 0)

	ms.GetRange(from, to, "id", func(item Item) bool {
		res = append(res, item.(TestStruct))
		return true
	})

	expected := idSortedData()[1:4]

	if !reflect.DeepEqual(res, expected) {
		t.Errorf("Get range with one index failed, result = %v\n expected = %v\n", res, expected)
	}
}

func TestRangeEmptyOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = TestStruct{id: 8}, TestStruct{id: 7}

	var res []TestStruct = make([]TestStruct, 0)

	ms.GetRange(from, to, "id", func(item Item) bool {
		res = append(res, item.(TestStruct))
		return true
	})

	if len(res) != 0 {
		t.Error("Get range with empty result one index failed")
	}
}

func TestRangeMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = TestStruct{importance: 2}, TestStruct{importance: 3.2}

	var res []TestStruct = make([]TestStruct, 0)

	ms.GetRange(from, to, "importance", func(item Item) bool {
		res = append(res, item.(TestStruct))
		return true
	})

	expected := importanceSortedData()[1:4]

	if !reflect.DeepEqual(res, expected) {
		t.Error("Get range with multiple index failed")
	}
}

func TestRangeEmptyMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = TestStruct{importance: 6}, TestStruct{importance: 7}

	var res []TestStruct = make([]TestStruct, 0)

	ms.GetRange(from, to, "importance", func(item Item) bool {
		res = append(res, item.(TestStruct))
		return true
	})

	if len(res) != 0 {
		t.Error("Get range with empty result multiple index failed")
	}
}

/*
	Max
*/

func TestMaxInvalidIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Max("notID")

	if result != nil {
		t.Error("Getting range with unspecified index didn't fail")
	}
}

func TestMaxOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Max("id").(TestStruct)
	sortedData := idSortedData()
	expected := sortedData[len(sortedData)-1]

	if !reflect.DeepEqual(result, expected) {
		t.Error("Get max result one index failed")
	}
}

func TestMaxEmptyOneIndex(t *testing.T) {
	ms := New([]string{"id"})

	result := ms.Max("id")

	if result != nil {
		t.Error("Get max, empty result one index failed")
	}
}

func TestMaxMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Max("importance").(TestStruct)
	sortedData := importanceSortedData()
	expected := sortedData[len(sortedData)-1]

	if !reflect.DeepEqual(result, expected) {
		t.Error("Get max result multiple index failed")
	}
}

func TestMaxEmptyMultipleIndex(t *testing.T) {
	ms := New([]string{"id", "importance"})

	result := ms.Max("importance")

	if result != nil {
		t.Error("Get max, empty result multiple index failed")
	}
}

/*
	Min
*/

func TestMinInvalidIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Min("notID")

	if result != nil {
		t.Error("Getting range with unspecified index didn't fail")
	}
}

func TestMinOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Min("id").(TestStruct)
	sortedData := idSortedData()
	expected := sortedData[0]

	if !reflect.DeepEqual(result, expected) {
		t.Error("Get max result one index failed")
	}
}

func TestMinEmptyOneIndex(t *testing.T) {
	ms := New([]string{"id"})

	result := ms.Min("id")

	if result != nil {
		t.Error("Get max, empty result one index failed")
	}
}

func TestMinMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Min("importance").(TestStruct)
	sortedData := importanceSortedData()
	expected := sortedData[0]

	if !reflect.DeepEqual(result, expected) {
		t.Error("Get max result multiple index failed")
	}
}

func TestMinEmptyMultipleIndex(t *testing.T) {
	ms := New([]string{"id", "importance"})

	result := ms.Min("importance")

	if result != nil {
		t.Error("Get max, empty result multiple index failed")
	}
}

/*
	Update Data
*/

func dataModifierFunc(i Item) (Item, bool) {
	itemCopy := i.(TestStruct)

	if itemCopy.name == "x" || itemCopy.name == "z" {
		itemCopy.name = "changed"
		return itemCopy, true
	} else {
		return itemCopy, false
	}
}

func TestUpdateData(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 1}
	result := ms.UpdateData(searchedRecord, "id", dataModifierFunc)

	if result == nil {
		t.Error("Update failed when it should succeed")
		return
	}

	if result.(TestStruct).name != "changed" {
		t.Error("First update not correct")
	}

	var searchedRecordByImportance Item = TestStruct{importance: 3}
	resultByImportance := ms.Get(searchedRecordByImportance, "importance").(TestStruct)

	if resultByImportance.name != "changed" {
		t.Error("Update did not propagate to other index trees")
	}

	var nonApplicableRecord Item = TestStruct{id: 9}
	nonApplicableResult := ms.UpdateData(nonApplicableRecord, "id", dataModifierFunc)

	if nonApplicableResult != nil {
		t.Error("Update didn't fail but function doesn't update record")
		return
	}

	var inexistentRecord Item = TestStruct{id: 100}
	inexistentRecordResult := ms.UpdateData(inexistentRecord, "id", dataModifierFunc)

	if inexistentRecordResult != nil {
		t.Error("Update didn't fail but record is not in store")
		return
	}
}

/*
	Update Data that can include index change
*/

func dataModifierFuncWithIndex(i Item) (Item, bool) {
	itemCopy := i.(TestStruct)

	if itemCopy.name == "x" || itemCopy.name == "z" {
		itemCopy.name = "changed"
		return itemCopy, true
	} else if itemCopy.name == "v" {
		itemCopy.id = -1
		return itemCopy, true
	} else {
		return itemCopy, false
	}
}

func TestUpdateWithIndexes(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = TestStruct{id: 1}
	result := ms.UpdateWithIndexes(searchedRecord, "id", dataModifierFuncWithIndex)

	if result == nil {
		t.Error("Update failed when it should succeed")
		return
	}

	if result.(TestStruct).name != "changed" {
		t.Error("First update not correct")
	}

	var searchedRecordByImportance Item = TestStruct{importance: 3}
	resultByImportance := ms.Get(searchedRecordByImportance, "importance").(TestStruct)

	if resultByImportance.name != "changed" {
		t.Error("Update did not propagate to other index trees")
	}

	var nonApplicableRecord Item = TestStruct{id: 8}
	nonApplicableResult := ms.UpdateWithIndexes(nonApplicableRecord, "id", dataModifierFuncWithIndex)

	if nonApplicableResult != nil {
		t.Error("Update didn't fail but function doesn't update record")
		return
	}

	var inexistentRecord Item = TestStruct{id: 100}
	inexistentRecordResult := ms.UpdateWithIndexes(inexistentRecord, "id", dataModifierFuncWithIndex)

	if inexistentRecordResult != nil {
		t.Error("Update didn't fail but record is not in store")
		return
	}

	var affectingIndex Item = TestStruct{id: 9}
	affectingIndexResult := ms.UpdateWithIndexes(affectingIndex, "id", dataModifierFuncWithIndex)

	if affectingIndexResult == nil || affectingIndexResult.(TestStruct).id != -1 {
		t.Error("Update affecting index didn't fail but function doesn't update record")
		return
	}

	resultMin := ms.Min("id")

	if resultMin == nil || resultMin.(TestStruct).id != -1 {
		t.Error("Update affecting index should readjust tables", resultMin.(TestStruct).id)
		return
	}
}

/*
	Update subset of data (does not include indexes)
*/

func dataSubsetModifierFunc(i Item) (Item, bool) {
	itemCopy := i.(TestStruct)

	if itemCopy.name == "x" || itemCopy.name == "y" || itemCopy.name == "z" {
		itemCopy.name = "changed"
		return itemCopy, true
	} else {
		return itemCopy, false
	}
}

func TestUpdateDataSubset(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _, v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	searchedRecords := []Item{TestStruct{id: 1}, TestStruct{id: 2}}
	result := ms.UpdateDataSubset(searchedRecords, "id", dataSubsetModifierFunc)

	if result == nil {
		t.Error("Update subset failed when it should succeed")
		return
	}

	if len(result) != 2 ||
		result[0].(TestStruct).name != "changed" ||
		result[1].(TestStruct).name != "changed" {
		t.Error("First update subset not correct")
	}

	searchedRecordsByImportance := []Item{TestStruct{importance: 3}, TestStruct{importance: 2}}
	resultByImportanceFirst := ms.Get(searchedRecordsByImportance[0], "importance")
	resultByImportanceSecond := ms.Get(searchedRecordsByImportance[1], "importance")

	if resultByImportanceFirst.(TestStruct).name != "changed" ||
		resultByImportanceSecond.(TestStruct).name != "changed" {
		t.Error("Update subset did not propagate to other index trees")
	}

	nonApplicableRecord := []Item{TestStruct{id: 9}}
	nonApplicableResult := ms.UpdateDataSubset(nonApplicableRecord, "id", dataSubsetModifierFunc)

	if !reflect.DeepEqual(nonApplicableResult, []Item{nil}) {
		t.Error("Update subset didn't fail but function doesn't update record")
		return
	}

	inexistentRecord := []Item{TestStruct{id: 100}}
	inexistentRecordResult := ms.UpdateDataSubset(inexistentRecord, "id", dataSubsetModifierFunc)

	if !reflect.DeepEqual(inexistentRecordResult, []Item{nil}) {
		t.Errorf("Update subset didn't fail but record is not in store")
		return
	}
}

/*
	Benchmarks
*/
type BenchStruct struct {
	id0 int
	id1 int
	id2 int
	id3 int
	id4 int
	id5 int
	id6 int
}

func (ts BenchStruct) Less(index string, than interface{}) bool {
	switch index {
	case "id0":
		return ts.id0 < than.(BenchStruct).id0
	case "id1":
		return ts.id1 < than.(BenchStruct).id1
	case "id2":
		return ts.id2 < than.(BenchStruct).id2
	case "id3":
		return ts.id3 < than.(BenchStruct).id3
	case "id4":
		return ts.id4 < than.(BenchStruct).id4
	case "id5":
		return ts.id5 < than.(BenchStruct).id5
	case "id6":
		return ts.id6 < than.(BenchStruct).id6
	default:
		return true
	}
}

func makeRandBenchStruct() *BenchStruct {
	return &BenchStruct{
		id0: rand.Intn(100000),
		id1: rand.Intn(100000),
		id2: rand.Intn(100000),
		id3: rand.Intn(100000),
		id4: rand.Intn(100000),
		id5: rand.Intn(100000),
		id6: rand.Intn(100000),
	}
}

func BenchmarkOneIndexInsert(b *testing.B) {
	ms := New([]string{"id0"})

	for n := 0; n < b.N; n++ {
		record := makeRandBenchStruct()
		ms.Add(*record)
	}
}

func BenchmarkSevenIndexInsert(b *testing.B) {
	ms := New([]string{"id0", "id1", "id2", "id3", "id4", "id5", "id6"})

	for n := 0; n < b.N; n++ {
		record := makeRandBenchStruct()
		ms.Add(*record)
	}
}

func BenchmarkOneIndexFind(b *testing.B) {
	ms := New([]string{"id0"})

	record := makeRandBenchStruct()
	for n := 0; n < 100000; n++ {
		ms.Add(*record)
		record = makeRandBenchStruct()
	}

	b.ResetTimer()

	var res Item
	for n := 0; n < b.N; n++ {
		res = ms.Get(*record, "id0")
		*record = res.(BenchStruct)
	}
}

func BenchmarkSevenIndexFind(b *testing.B) {
	ms := New([]string{"id0", "id1", "id2", "id3", "id4", "id5", "id6"})

	record := makeRandBenchStruct()
	for n := 0; n < 100000; n++ {
		ms.Add(*record)
		record = makeRandBenchStruct()
	}

	b.ResetTimer()

	var res Item
	for n := 0; n < b.N; n++ {
		res = ms.Get(*record, "id0")
		*record = res.(BenchStruct)
	}
}
