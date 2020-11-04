/*
 * Copyright © 2020 stepsman authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bl

import (
	"fmt"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/dao"
	_ "github.com/jackc/pgx/stdlib"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
)

func InitBL(daoParameters *dao.ParametersType) error {
	err := dao.InitDAO(daoParameters)
	if err != nil {
		return err
	}
	if !dao.IsRemote {
		err = MigrateDB(daoParameters.DatabaseAutoMigrate)
		if err != nil {
			return fmt.Errorf("failed to init: %w", err)
		}
	} else {
		if viper.IsSet("MAX_RESPONSE_HEADER_BYTES") {
			initDO(viper.GetInt64("MAX_RESPONSE_HEADER_BYTES"))
		}
		client.InitClient(dao.Parameters.DatabaseSSLMode, dao.Parameters.DatabaseHost, dao.Parameters.DatabasePort)
	}
	return nil
}

func InitLogrus(out io.Writer) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.TraceLevel)
	log.SetOutput(out)
}
