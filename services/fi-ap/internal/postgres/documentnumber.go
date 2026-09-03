package postgres

import (
	"context"
	"fmt"
)

func (s *Store) NextBatch(ctx context.Context, count int) ([]string, error) {
	if count < 0 {
		return nil, fmt.Errorf("document count must be >= 0")
	}
	if count == 0 {
		return []string{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT lpad(nextval('fi_document_number_seq')::text, 10, '0')
		FROM generate_series(1, $1)
	`, count)
	if err != nil {
		return nil, fmt.Errorf("nextval fi_document_number_seq: %w", err)
	}
	defer rows.Close()

	numbers := make([]string, 0, count)
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		numbers = append(numbers, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(numbers) != count {
		return nil, fmt.Errorf("nextval: got %d numbers, want %d", len(numbers), count)
	}
	return numbers, nil
}
