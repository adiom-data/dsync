/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package mongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/cespare/xxhash"
)

func (mc *BaseMongoConnector) IntegrityCheck(ctx context.Context, query iface.IntegrityCheckQuery) (iface.ConnectorDataIntegrityCheckResult, error) {
	filter := createFindFilter(query)
	collection := mc.Client.Database(query.Db).Collection(query.Col)

	if query.CountOnly {
		c, err := collection.EstimatedDocumentCount(mc.Ctx)
		if err != nil {
			if errors.Is(mc.Ctx.Err(), context.Canceled) {
				slog.Debug(fmt.Sprintf("Count error: %v, but the context was cancelled", err))
			} else {
				slog.Error(fmt.Sprintf("Failed to count documents: %v", err))
			}
			return iface.ConnectorDataIntegrityCheckResult{}, err
		}
		return iface.ConnectorDataIntegrityCheckResult{Count: c}, nil
	}

	hasher := xxhash.New()
	var hash uint64
	var count int64
	c, err := collection.Find(mc.Ctx, filter)
	if err != nil {
		if !errors.Is(mc.Ctx.Err(), context.Canceled) {
			slog.Error(fmt.Sprintf("Failed to fetch documents for integrity check: %v", err))
		}
		return iface.ConnectorDataIntegrityCheckResult{}, err
	}
	for c.Next(mc.Ctx) {
		hasher.Reset()
		_, err := hasher.Write(c.Current)
		if err != nil {
			slog.Error(fmt.Sprintf("Error hashing during integrity check: %v", err))
			return iface.ConnectorDataIntegrityCheckResult{}, err
		}
		hash ^= hasher.Sum64()
		count += 1
	}
	return iface.ConnectorDataIntegrityCheckResult{
		XXHash: hash,
		Count:  count,
	}, nil
}
