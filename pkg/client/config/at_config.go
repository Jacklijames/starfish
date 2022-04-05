package config

import (
	"time"
)

type ATConfig struct {
	DSN                 string        `yaml:"dsn" json:"dsn,omitempty"`
	ReportRetryCount    int           `default:"5" yaml:"report_retry_count" json:"report_retry_count,omitempty"`
	ReportSuccessEnable bool          `default:"false" yaml:"report_success_enable" json:"report_success_enable,omitempty"`
	LockRetryInterval   time.Duration `default:"10ms" yaml:"lock_retry_interval" json:"lock_retry_interval,omitempty"`
	LockRetryTimes      int           `default:"30" yaml:"lock_retry_times" json:"lock_retry_times,omitempty"`
}
