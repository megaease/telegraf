package mongodb

import (
	"testing"

	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

func TestMongodb_SkipPingAtInit(t *testing.T) {
	m := &MongoDB{
		Log:     testutil.Logger{},
		Servers: []string{"mongodb://aaa:bbb@127.0.0.1:37017/adminaaa"},
	}
	err := m.Init()
	assert.Error(t, err)
	m.SkipPingAtInit = true
	err = m.Init()
	assert.NoError(t, err)
}
