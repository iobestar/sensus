package app

import (
	"context"
	"github.com/iobestar/sensus/internal"
	"github.com/iobestar/sensus/internal/app/db"
	"github.com/iobestar/sensus/internal/app/web"
	"github.com/iobestar/sensus/internal/database"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
)

type App interface {
	internal.Runner
	http.Handler
}

type app struct {
	echo *echo.Echo
}

func New() (App, error) {
	return &app{
		echo: echo.New(),
	}, nil
}

func (a *app) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	a.echo.ServeHTTP(writer, request)
}

func (a *app) Run(_ context.Context, _ *sync.WaitGroup, disposer internal.Disposer) error {
	databaseCfg := database.NewConfig()
	if _, ok := databaseCfg.(database.NoneConfig); !ok {
		if err := database.Migrate(db.Migration, databaseCfg); nil != err {
			return errors.Wrap(err, "error database migrate")
		}

		dbx, err := database.Connect(databaseCfg)
		if nil != err {
			return errors.Wrap(err, "error database connect")
		}

		disposer.Attach(func() {
			if err := dbx.Close(); nil != err {
				log.WithError(err).Error("error database close")
			}
		})
	}

	appCtx := NewContext(NewConfig())
	if err := appCtx.Init(); nil != err {
		return errors.Wrap(err, "error app context init")
	}

	if err := appCtx.Start(); nil != err {
		return errors.Wrap(err, "error app context start")
	}
	disposer.Attach(func() {
		if err := appCtx.Stop(); nil != err {
			log.WithError(err).Error("error app context stop")
		}
	})

	return web.Setup(a.echo, appCtx.Controllers()...)
}
