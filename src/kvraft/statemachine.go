package kvraft

type StateMachine interface {
	// Get fetches the current value for the key.
	// A Get for a non-existent key should return an empty string.
	Get(key string) (string, Err)

	// Put replaces the value for a particular key in the database.
	Put(key, value string) Err

	// Append arg to key's value.
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		KV: make(map[string]string),
	}
}

func (m *MemoryKV) Get(key string) (string, Err) {
	if value, ok := m.KV[key]; ok {
		return value, OK
	}

	return "", ErrNoKey
}

func (m *MemoryKV) Put(key, value string) Err {
	m.KV[key] = value
	return OK
}

func (m *MemoryKV) Append(key, value string) Err {
	m.KV[key] += value
	return OK
}
