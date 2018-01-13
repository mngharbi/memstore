package memstore

import (
	"testing"
	"math/rand"
	"reflect"
)

type TestStruct struct {
	id int
	importance float32
}

// Comparator for multiple keys
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

func testData() []*TestStruct {
	return []*TestStruct{
		&TestStruct{1, 3},
		&TestStruct{2, 2},
		&TestStruct{3, 5},
		&TestStruct{4, 0},
		&TestStruct{8, 3.2},
		&TestStruct{9, 3.1},
	}
}

func idSortedData() []*TestStruct {
	return testData()
}

func importanceSortedData() []*TestStruct {
	return []*TestStruct{
		&TestStruct{4, 0},
		&TestStruct{2, 2},
		&TestStruct{1, 3},
		&TestStruct{9, 3.1},
		&TestStruct{8, 3.2},
		&TestStruct{3, 5},
	}
}

func shuffeledTestData(data []*TestStruct) []*TestStruct {
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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
		t.Error("Get range with one index failed")
	}
}

func TestRangeEmptyOneIndex(t *testing.T) {
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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

func TestMaxOneIndex(t *testing.T) {
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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
	data := testData()

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

