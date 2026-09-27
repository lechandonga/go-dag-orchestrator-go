package dag

import "fmt"

// Validate 校验编排定义：任务 ID 唯一且非空、依赖存在、无环。
// 返回 nil 表示合法；否则返回 *ValidationError，可据此区分环与缺失依赖。
func Validate(spec Spec) error {
	ids := make(map[string]bool, len(spec.Tasks))
	for _, t := range spec.Tasks {
		if t.ID == "" {
			return &ValidationError{Kind: FailureValidation, Detail: "empty task id"}
		}
		if ids[t.ID] {
			return &ValidationError{Kind: FailureValidation, Detail: "duplicate task id", TaskIDs: []string{t.ID}}
		}
		ids[t.ID] = true
	}
	for _, t := range spec.Tasks {
		for _, dep := range t.Deps {
			if !ids[dep] {
				return &ValidationError{
					Kind:    FailureValidation,
					Detail:  fmt.Sprintf("task %q depends on missing task %q", t.ID, dep),
					TaskIDs: []string{t.ID, dep},
				}
			}
		}
	}
	if cycle := findCycle(spec); cycle != nil {
		return &ValidationError{Kind: FailureValidation, Detail: "dependency cycle detected", TaskIDs: cycle}
	}
	return nil
}

// findCycle 用三色 DFS 找环，返回环上的任务 ID；无环返回 nil。
func findCycle(spec Spec) []string {
	deps := make(map[string][]string, len(spec.Tasks))
	for _, t := range spec.Tasks {
		deps[t.ID] = t.Deps
	}
	const (
		white = 0 // 未访问
		gray  = 1 // 在栈上
		black = 2 // 已完成
	)
	color := make(map[string]int, len(spec.Tasks))
	var stack []string
	var cycle []string
	var visit func(id string) bool
	visit = func(id string) bool {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range deps[id] {
			switch color[dep] {
			case gray:
				// 找到环：截取栈中从 dep 开始的部分
				for i, s := range stack {
					if s == dep {
						cycle = append([]string(nil), stack[i:]...)
						return true
					}
				}
			case white:
				if visit(dep) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return false
	}
	for _, t := range spec.Tasks {
		if color[t.ID] == white {
			if visit(t.ID) {
				return cycle
			}
		}
	}
	return nil
}

// topoLayers 按依赖层次对任务 ID 分组：第 0 层无依赖，第 i 层只依赖更浅的层。
// 前置条件：spec 已通过 Validate。
func topoLayers(spec Spec) ([][]string, error) {
	indegree := make(map[string]int, len(spec.Tasks))
	children := make(map[string][]string, len(spec.Tasks))
	for _, t := range spec.Tasks {
		indegree[t.ID] = len(t.Deps)
		for _, dep := range t.Deps {
			children[dep] = append(children[dep], t.ID)
		}
	}
	var layers [][]string
	remaining := len(spec.Tasks)
	for remaining > 0 {
		var layer []string
		for _, t := range spec.Tasks {
			if indegree[t.ID] == 0 {
				layer = append(layer, t.ID)
			}
		}
		if len(layer) == 0 {
			return nil, &ValidationError{Kind: FailureValidation, Detail: "dependency cycle detected"}
		}
		for _, id := range layer {
			indegree[id] = -1 // 标记已出层
			for _, c := range children[id] {
				indegree[c]--
			}
		}
		remaining -= len(layer)
		layers = append(layers, layer)
	}
	return layers, nil
}
