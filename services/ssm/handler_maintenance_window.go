package ssm

func (h *Handler) ssmMaintenanceWindowOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DeleteMaintenanceWindow":               jsonOp(h.Backend.DeleteMaintenanceWindow),
		"DeregisterTargetFromMaintenanceWindow": jsonOp(h.Backend.DeregisterTargetFromMaintenanceWindow),
		"DeregisterTaskFromMaintenanceWindow":   jsonOp(h.Backend.DeregisterTaskFromMaintenanceWindow),
		"DescribeMaintenanceWindowExecutionTaskInvocations": jsonOp(
			h.Backend.DescribeMaintenanceWindowExecutionTaskInvocations,
		),
		"DescribeMaintenanceWindowExecutionTasks":     jsonOp(h.Backend.DescribeMaintenanceWindowExecutionTasks),
		"DescribeMaintenanceWindowExecutions":         jsonOp(h.Backend.DescribeMaintenanceWindowExecutions),
		"DescribeMaintenanceWindowSchedule":           jsonOp(h.Backend.DescribeMaintenanceWindowSchedule),
		"DescribeMaintenanceWindowTargets":            jsonOp(h.Backend.DescribeMaintenanceWindowTargets),
		"DescribeMaintenanceWindowTasks":              jsonOp(h.Backend.DescribeMaintenanceWindowTasks),
		"DescribeMaintenanceWindows":                  jsonOp(h.Backend.DescribeMaintenanceWindows),
		"DescribeMaintenanceWindowsForTarget":         jsonOp(h.Backend.DescribeMaintenanceWindowsForTarget),
		"GetMaintenanceWindow":                        jsonOp(h.Backend.GetMaintenanceWindow),
		"GetMaintenanceWindowExecution":               jsonOp(h.Backend.GetMaintenanceWindowExecution),
		"GetMaintenanceWindowExecutionTask":           jsonOp(h.Backend.GetMaintenanceWindowExecutionTask),
		"GetMaintenanceWindowExecutionTaskInvocation": jsonOp(h.Backend.GetMaintenanceWindowExecutionTaskInvocation),
		"GetMaintenanceWindowTask":                    jsonOp(h.Backend.GetMaintenanceWindowTask),
		"RegisterTargetWithMaintenanceWindow":         jsonOp(h.Backend.RegisterTargetWithMaintenanceWindow),
		"RegisterTaskWithMaintenanceWindow":           jsonOp(h.Backend.RegisterTaskWithMaintenanceWindow),
		"UpdateMaintenanceWindow":                     jsonOp(h.Backend.UpdateMaintenanceWindow),
		"UpdateMaintenanceWindowTarget":               jsonOp(h.Backend.UpdateMaintenanceWindowTarget),
		"UpdateMaintenanceWindowTask":                 jsonOp(h.Backend.UpdateMaintenanceWindowTask),
		"CancelMaintenanceWindowExecution":            jsonOp(h.Backend.CancelMaintenanceWindowExecution),
		"CreateMaintenanceWindow":                     jsonOp(h.Backend.CreateMaintenanceWindow),
	}
}
