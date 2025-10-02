package ds

func NewFrontier() *frontierImpl {
	return &frontierImpl{
		Items: map[string]*ListNode{},
	}
}

type frontierImpl struct {
	Newest *ListNode
	Oldest *ListNode
	Items  map[string]*ListNode
}

func (f *frontierImpl) Put(k string, v *Node, t int64) {
	if f.Newest == nil {
		newNode := &ListNode{
			Node:      v,
			Timestamp: t,
		}
		f.Newest = newNode
		f.Oldest = newNode
		f.Items[k] = newNode
	} else {
		cur, ok := f.Items[k]
		if !ok {
			newNode := &ListNode{
				Node:      v,
				Timestamp: t,
				Older:     f.Newest,
			}
			f.Newest.Newer = newNode
			f.Newest = newNode
			f.Items[k] = newNode
		} else if f.Newest == cur {
			cur.Timestamp = t
		} else if f.Oldest == cur {
			cur.Timestamp = t
			f.Oldest = cur.Newer
			cur.Newer.Older = nil
			cur.Newer = nil
			cur.Older = f.Newest
			f.Newest.Newer = cur
			f.Newest = cur
		} else {
			cur.Timestamp = t
			cur.Newer.Older = cur.Older
			cur.Older.Newer = cur.Newer
			cur.Newer = nil
			cur.Older = f.Newest
			f.Newest.Newer = cur
			f.Newest = cur
		}
	}
}

func (f *frontierImpl) Check(k string) bool {
	_, ok := f.Items[k]
	return ok
}

func (f *frontierImpl) Delete(t int64, limit int) []*Node {
	if f.Oldest != nil {
		var res []*Node
		cur := f.Oldest
		for cur != nil && cur.Timestamp <= t && (limit == 0 || len(res) < limit) {
			res = append(res, cur.Node)
			delete(f.Items, cur.Node.ID)
			cur = cur.Newer
		}
		if cur == nil {
			f.Newest = nil
		} else {
			cur.Older = nil
		}
		f.Oldest = cur
		return res
	}
	return nil
}

type ListNode struct {
	*Node
	Timestamp int64
	Newer     *ListNode
	Older     *ListNode
}
