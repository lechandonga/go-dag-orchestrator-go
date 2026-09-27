package engine

import "time"

func (s *FileStore) nowForTest() time.Time { return time.Now().UTC() }
