package memstore

import (
	"testing"
	"math/rand"
	"reflect"
)

type TestStruct struct {
	id int
	importance float32
	name string
}

type VTestStruct struct {
	id int
	importance float32
	name string
}

// Comparator for multiple keys for values
func (ts *TestStruct) Less(index string, than interface{}) bool {
	switch(index) {
		case "id":
			return ts.id < than.(*TestStruct).id
		case "importance":
			return ts.importance < than.(*TestStruct).importance
		default:
			return true
	}
}

// Comparator for multiple keys for values (with values)
func (ts VTestStruct) Less(index string, than interface{}) bool {
	switch(index) {
		case "id":
			return ts.id < than.(VTestStruct).id
		case "importance":
			return ts.importance < than.(VTestStruct).importance
		default:
			return true
	}
}

func testData() []*TestStruct {
	return []*TestStruct{
		&TestStruct{1, 3, "x"},
		&TestStruct{2, 2, "y"},
		&TestStruct{3, 5, "z"},
		&TestStruct{4, 0, "t"},
		&TestStruct{8, 3.2, "u"},
		&TestStruct{9, 3.1, "v"},
	}
}

func testDataByValue() []VTestStruct {
	return []VTestStruct{
		VTestStruct{1, 3, "x"},
		VTestStruct{2, 2, "y"},
		VTestStruct{3, 5, "z"},
		VTestStruct{4, 0, "t"},
		VTestStruct{8, 3.2, "u"},
		VTestStruct{9, 3.1, "v"},
	}
}

func idSortedData() []*TestStruct {
	return testData()
}

func idSortedDataByValue() []VTestStruct {
	return testDataByValue()
}

func importanceSortedData() []*TestStruct {
	return []*TestStruct{
		&TestStruct{4, 0, "t"},
		&TestStruct{2, 2, "y"},
		&TestStruct{1, 3, "x"},
		&TestStruct{9, 3.1, "v"},
		&TestStruct{8, 3.2, "u"},
		&TestStruct{3, 5, "z"},
	}
}

func importanceSortedDataByValue() []VTestStruct {
	return []VTestStruct{
		VTestStruct{4, 0, "t"},
		VTestStruct{2, 2, "y"},
		VTestStruct{1, 3, "x"},
		VTestStruct{9, 3.1, "v"},
		VTestStruct{8, 3.2, "u"},
		VTestStruct{3, 5, "z"},
	}
}

func shuffeledTestData() (data []*TestStruct) {
	data = testData()

	for i := range data {
    	j := rand.Intn(i + 1)
    	data[i], data[j] = data[j], data[i]
	}

	return data
}

func shuffeledTestDataByValue() (data []VTestStruct) {
	data = testDataByValue()

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
	for _,v := range data {
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	if ms.Len() != len(data) {
		t.Error("Adding with multiple index failed")
	}
}

/*
	Delete
*/

func TestDeleteInvalidIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 3}
	result := ms.Delete(searchedRecord, "notID")

	if result != nil {
		t.Error("Deleting with unspecified index didn't fail")
	}
}


// One index, Success
func TestDeleteOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 3}
	result := ms.Delete(searchedRecord, "id").(*TestStruct)


	if(ms.Len() != len(data)-1 ||
		result.id != 3 || result.importance != 5) {
		t.Error("Deleting with one index failed")
	}
}

// One index, Empty
func TestDeleteEmptyOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 10}
	result := ms.Delete(searchedRecord, "id")

	if result != nil {
		t.Error("Deleting inexistent with one index failed")
	}
}

// Multiple index, Success
func TestDeleteMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{importance: 5}
	result := ms.Delete(searchedRecord, "importance").(*TestStruct)

	if(ms.Len() != len(data)-1 ||
		result.id != 3 || result.importance != 5) {
		t.Error("Deleting with multiple index failed")
	}
}

// Multiple index, Empty
func TestDeleteEmptyMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{importance: 6}
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 3}
	result := ms.Get(searchedRecord, "notID")

	if result != nil {
		t.Error("Getting with unspecified index didn't fail")
	}
}

// One index, Success
func TestGetOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 3}

	result := ms.Get(searchedRecord, "id").(*TestStruct)

	if result.id != 3 || result.importance != 5 {
		t.Error("Get with one index failed")
	}
}

// One index, Empty
func TestGetEmptyOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 10}

	result := ms.Get(searchedRecord, "id")

	if result != nil {
		t.Error("Get inexistent with one index failed")
	}
}

// Multiple index, Success
func TestGetMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{importance: 5}
	result := ms.Get(searchedRecord, "importance").(*TestStruct)

	if result.id != 3 || result.importance != 5 {
		t.Error("Get with multiple index failed")
	}
}

// Multiple index, Empty
func TestGetEmptyMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{importance: 6}
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = &TestStruct{id: 2}, &TestStruct{id: 7}

	var res []*TestStruct = make([]*TestStruct, 0)

	ms.GetRange(from, to, "notID", func(item Item) bool {
		res = append(res, item.(*TestStruct))
		return true
	})

	if len(res) != 0 {
		t.Error("Getting range with unspecified index didn't fail")
	}
}

func TestRangeOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = &TestStruct{id: 2}, &TestStruct{id: 7}

	var res []*TestStruct = make([]*TestStruct, 0)

	ms.GetRange(from, to, "id", func(item Item) bool {
		res = append(res, item.(*TestStruct))
		return true
	})

	expected := idSortedData()[1:4]

	if !reflect.DeepEqual(res, expected) {
		t.Errorf("Get range with one index failed, result = %v\n expected = %v\n", []TestStruct{*res[0], *res[1], *res[2]}, []TestStruct{*expected[0], *expected[1], *expected[2]})
	}
}

func TestRangeEmptyOneIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = &TestStruct{id: 8}, &TestStruct{id: 7}

	var res []*TestStruct = make([]*TestStruct, 0)

	ms.GetRange(from, to, "id", func(item Item) bool {
		res = append(res, item.(*TestStruct))
		return true
	})

	if len(res) != 0 {
		t.Error("Get range with empty result one index failed")
	}
}

func TestRangeMultipleIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = &TestStruct{importance: 2}, &TestStruct{importance: 3.2}

	var res []*TestStruct = make([]*TestStruct, 0)

	ms.GetRange(from, to, "importance", func(item Item) bool {
		res = append(res, item.(*TestStruct))
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var from, to Item = &TestStruct{importance: 6}, &TestStruct{importance: 7}

	var res []*TestStruct = make([]*TestStruct, 0)

	ms.GetRange(from, to, "importance", func(item Item) bool {
		res = append(res, item.(*TestStruct))
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
	for _,v := range data {
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Max("id").(*TestStruct)
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Max("importance").(*TestStruct)
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
	for _,v := range data {
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Min("id").(*TestStruct)
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
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	result := ms.Min("importance").(*TestStruct)
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

func dataModifierFunc(i interface{}) (interface{}, bool) {
	itemPtrCopy := i.(*TestStruct)

	if(itemPtrCopy.name == "x" || itemPtrCopy.name == "z") {
		itemPtrCopy.name = "changed"
		return itemPtrCopy, true
	} else {
		return itemPtrCopy, false
	}
}

func TestUpdateDataWithPointers(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 1}
	result := ms.UpdateData(searchedRecord, "id", dataModifierFunc)

	if result == nil {
		t.Error("Update failed when it should succeed")
		return
	}

	if result.(*TestStruct).name != "changed" {
		t.Error("First update not correct")
	}

	var searchedRecordByImportance Item = &TestStruct{importance: 3}
	resultByImportance := ms.Get(searchedRecordByImportance, "importance").(*TestStruct)

	if resultByImportance.name != "changed" {
		t.Error("Update did not propagate to other index trees")
	}

	var nonApplicableRecord Item = &TestStruct{id: 9}
	nonApplicableResult := ms.UpdateData(nonApplicableRecord, "id", dataModifierFunc)

	if nonApplicableResult != nil {
		t.Error("Update didn't fail but function doesn't update record")
		return
	}

	var inexistentRecord Item = &TestStruct{id: 100}
	inexistentRecordResult := ms.UpdateData(inexistentRecord, "id", dataModifierFunc)

	if inexistentRecordResult != nil {
		t.Error("Update didn't fail but record is not in store")
		return
	}
}

func dataModifierFuncWithValues(i interface{}) (interface{}, bool) {
	itemCopy := i.(VTestStruct)
	if(itemCopy.name == "x" || itemCopy.name == "z") {
		itemCopy.name = "changed"
		return itemCopy, true
	} else {
		return itemCopy, false
	}
}

func TestUpdateDataWithValues(t *testing.T) {
	data := shuffeledTestDataByValue()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = VTestStruct{id: 1}
	result := ms.UpdateData(searchedRecord, "id", dataModifierFuncWithValues)

	if result == nil {
		t.Error("Update failed when it should succeed")
		return
	}

	if result.(VTestStruct).name != "changed" {
		t.Error("First update not correct")
	}

	var searchedRecordByImportance Item = VTestStruct{importance: 3}
	resultByImportance := ms.Get(searchedRecordByImportance, "importance").(VTestStruct)

	if resultByImportance.name != "changed" {
		t.Error("Update did not propagate to other index trees")
	}

	var nonApplicableRecord Item = VTestStruct{id: 9}
	nonApplicableResult := ms.UpdateData(nonApplicableRecord, "id", dataModifierFuncWithValues)

	if nonApplicableResult != nil {
		t.Error("Update didn't fail but function doesn't update record")
		return
	}

	var inexistentRecord Item = VTestStruct{id: 100}
	inexistentRecordResult := ms.UpdateData(inexistentRecord, "id", dataModifierFuncWithValues)

	if inexistentRecordResult != nil {
		t.Error("Update didn't fail but record is not in store")
		return
	}
}

func TestUpdateDataInvalidIndex(t *testing.T) {
	data := shuffeledTestData()

	ms := New([]string{"id", "importance"})
	for _,v := range data {
		var vItem Item = v
		ms.Add(vItem)
	}

	var searchedRecord Item = &TestStruct{id: 1}
	result := ms.UpdateData(searchedRecord, "random", dataModifierFunc)

	if result != nil {
		t.Error("Update didn't fail with incorrect index")
		return
	}
}
