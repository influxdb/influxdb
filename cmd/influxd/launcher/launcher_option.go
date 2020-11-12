package launcher

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type Option interface {
	applyInit(l *Launcher)
	applyConfig(l *Launcher)
}

func WithLogger(log *zap.Logger) Option {
	return &launcherOption{
		applyConfigFn: func(l *Launcher) {
			l.log = log
		},
	}
}

func WithViper(v *viper.Viper) Option {
	return &launcherOption{
		applyInitFn: func(l *Launcher) {
			l.Viper = v
		},
	}
}

type launcherOption struct {
	applyInitFn   func(*Launcher)
	applyConfigFn func(*Launcher)
}

var _ Option = launcherOption{}

func (o launcherOption) applyConfig(l *Launcher) {
	if o.applyConfigFn != nil {
		o.applyConfigFn(l)
	}
}

func (o launcherOption) applyInit(l *Launcher) {
	if o.applyInitFn != nil {
		o.applyInitFn(l)
	}
}
