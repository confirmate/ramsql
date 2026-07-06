package agnostic

import (
	"container/list"
	"fmt"
)

type IndexSrc struct {
	tuples []*list.Element
	pos    int
	rname  string
	cols   []string
}

func NewHashIndexSource(index Index, alias string, p Predicate) (*IndexSrc, error) {
	s := &IndexSrc{}

	i, ok := index.(*HashIndex)
	if !ok {
		return nil, fmt.Errorf("index %s is not a HashIndex", index)
	}
	s.rname = i.relName
	s.cols = i.relAttrs

	if alias != "" {
		s.rname = alias
	}

	eq, ok := p.(*EqPredicate)
	if !ok {
		return nil, fmt.Errorf("predicate %s is not a Eq predicate", p)
	}

	tuples, err := i.GetAll([]any{eq.right.Value(nil, nil)})
	if err != nil {
		return nil, fmt.Errorf("cannot create NewHashIndexSource(%s,%s): %s", index, p, err)
	}

	s.tuples = tuples
	return s, nil
}

func (s IndexSrc) String() string {
	return "IndexScan on " + s.rname
}

func (s *IndexSrc) HasNext() bool {
	return s.pos < len(s.tuples)
}

func (s *IndexSrc) Next() *list.Element {
	if s.pos >= len(s.tuples) {
		return nil
	}
	e := s.tuples[s.pos]
	s.pos++
	return e
}

func (s *IndexSrc) Columns() []string {
	return s.cols
}

func (s *IndexSrc) EstimateCardinal() int64 {
	return int64(len(s.tuples))
}

type SeqScanSrc struct {
	e     *list.Element
	card  int64
	rname string
	cols  []string
}

func NewSeqScan(r *Relation, alias string) *SeqScanSrc {
	s := &SeqScanSrc{
		e:     r.rows.Front(),
		card:  int64(r.rows.Len()),
		rname: r.name,
	}
	if alias != "" {
		s.rname = alias
	}
	for _, a := range r.attributes {
		s.cols = append(s.cols, a.name)
	}
	return s
}

func (s SeqScanSrc) String() string {
	return "SeqScan on " + s.rname
}

func (s *SeqScanSrc) HasNext() bool {
    return s.e != nil
}

func (s *SeqScanSrc) Next() *list.Element {
	if s.e == nil {
		return nil
	}
	t := s.e
	s.e = s.e.Next()
	return t
}

func (s *SeqScanSrc) EstimateCardinal() int64 {
	return s.card
}

func (s *SeqScanSrc) Columns() []string {
	return s.cols
}
