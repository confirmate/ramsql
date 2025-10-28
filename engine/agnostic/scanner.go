package agnostic

import (
	"container/list"
	"fmt"
)

type RelationScanner struct {
	src        Source
	predicates []Predicate
}

func NewRelationScanner(src Source, predicates []Predicate) *RelationScanner {
	s := &RelationScanner{
		src:        src,
		predicates: predicates,
	}

	return s
}

func (s RelationScanner) String() string {
	return fmt.Sprintf("scan %s with %s", s.src, s.predicates)
}

func (s *RelationScanner) Append(p Predicate) {
	s.predicates = append(s.predicates, p)
}

func (s *RelationScanner) Exec() ([]string, []*list.Element, error) {
	var ok bool
	var err error
	var res []*list.Element
	var canAppend bool

	cols := s.src.Columns()
	for s.src.HasNext() {
		t := s.src.Next()
		canAppend = true
		for _, p := range s.predicates {
			ok, err = p.Eval(cols, t.Value.(*Tuple))
			if err != nil {
				return nil, nil, fmt.Errorf("RelationScanner.Exec: %s(%v) : %w", p, t, err)
			}
			if !ok {
				canAppend = false
				break
			}
		}
		if canAppend {
			res = append(res, t)
		}
	}

	return cols, res, nil
}

// No idea on how to estimate cardinal of scanner given predicates
//
// min: 0
// max: len(src)
// avg: len(src)/2
func (s *RelationScanner) EstimateCardinal() int64 {
	if len(s.predicates) == 0 {
		return s.src.EstimateCardinal()
	}

	return int64(s.src.EstimateCardinal()/2) + 1
}

func (s *RelationScanner) Children() []Node {
	return nil
}

// SingleRowScanner generates a single empty row (for SELECT without FROM)
// This allows ConstSelectors to work through the normal SelectorNode pipeline
type SingleRowScanner struct{}

func NewSingleRowScanner() *SingleRowScanner {
	return &SingleRowScanner{}
}

func (s SingleRowScanner) String() string {
	return "single row (SELECT without FROM)"
}

func (s *SingleRowScanner) Exec() ([]string, []*list.Element, error) {
	// Return a single empty tuple with no columns
	l := list.New()
	l.PushBack(NewTuple())
	return []string{}, []*list.Element{l.Front()}, nil
}

func (s *SingleRowScanner) EstimateCardinal() int64 {
	return 1 // Always returns exactly one row
}

func (s *SingleRowScanner) Children() []Node {
	return nil
}
