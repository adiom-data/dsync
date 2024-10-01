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
)

func (mc *BaseMongoConnector) IntegrityCheck(ctx context.Context, flowId iface.FlowID, task iface.ReadPlanTask) (iface.ConnectorDataIntegrityCheckResult, error) {
	filter := createFindFilter(task)
	collection := mc.Client.Database(task.Def.Db).Collection(task.Def.Col)
	c, err := collection.CountDocuments(mc.Ctx, filter)
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
