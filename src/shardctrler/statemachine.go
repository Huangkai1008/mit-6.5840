package shardctrler

import "sort"

type ConfigStateMachine interface {
	Join(servers map[int][]string) Err
	Leave(gids []int) Err
	Move(shard int, gid int) Err
	Query(num int) (Config, Err)
}

type MemoryConfigStateMachine struct {
	configs []Config
}

func newMemoryConfigStateMachine() *MemoryConfigStateMachine {
	configs := make([]Config, 1)
	configs[0] = Config{
		Groups: make(map[int][]string),
	}
	return &MemoryConfigStateMachine{
		configs: configs,
	}
}

func (m *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	lastConfig := m.configs[len(m.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: deepCopyGroups(lastConfig.Groups),
	}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newConfig.Groups[gid] = make([]string, len(servers))
			copy(newConfig.Groups[gid], servers)
		}
	}

	g2s := group2Shards(newConfig)

	for {
		source, target := getGIDWithMaxShards(g2s), getGIDWithMinShards(g2s)
		if source != 0 && len(g2s[source])-len(g2s[target]) <= 1 {
			break
		}
		g2s[target] = append(g2s[target], g2s[source][0])
		g2s[source] = g2s[source][1:]
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shardId := range shards {
			newShards[shardId] = gid
		}
	}
	newConfig.Shards = newShards
	m.configs = append(m.configs, newConfig)
	return OK
}

func (m *MemoryConfigStateMachine) Leave(gids []int) Err {
	lastConfig := m.configs[len(m.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: deepCopyGroups(lastConfig.Groups),
	}

	g2s := group2Shards(newConfig)
	orphanShards := make([]int, 0)

	for _, gid := range gids {
		delete(newConfig.Groups, gid)

		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		for _, shard := range orphanShards {
			target := getGIDWithMinShards(g2s)
			g2s[target] = append(g2s[target], shard)
		}

		for gid, shards := range g2s {
			for _, shardId := range shards {
				newShards[shardId] = gid
			}
		}
	}
	newConfig.Shards = newShards
	m.configs = append(m.configs, newConfig)
	return OK
}

func (m *MemoryConfigStateMachine) Move(shard int, gid int) Err {
	lastConfig := m.configs[len(m.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: deepCopyGroups(lastConfig.Groups),
	}

	newConfig.Shards[shard] = gid
	m.configs = append(m.configs, newConfig)
	return OK
}

func (m *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	// If the number is -1 or bigger than the biggest known configuration number,
	// the shardctrler should reply with the latest configuration.
	if num < 0 || num > len(m.configs) {
		return m.configs[len(m.configs)-1], OK
	}

	return m.configs[num], OK
}

func deepCopyGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for k, v := range groups {
		newGroups[k] = make([]string, len(v))
		copy(newGroups[k], v)
	}
	return newGroups
}

func group2Shards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

func getGIDWithMinShards(g2s map[int][]int) int {
	gids := make([]int, len(g2s))
	for gid := range g2s {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minLen, minGID := NShards+1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < minLen {
			minLen = len(g2s[gid])
			minGID = gid
		}
	}
	return minGID
}

func getGIDWithMaxShards(g2s map[int][]int) int {
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}

	gids := make([]int, len(g2s))
	for gid := range g2s {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxLen, maxGID := -1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) > maxLen {
			maxLen = len(g2s[gid])
			maxGID = gid
		}
	}
	return maxGID
}
