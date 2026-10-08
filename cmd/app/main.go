package main

import (
	"app/internal"
	"app/internal/boot"
	"app/internal/controllers"
	"app/internal/db"
	"app/internal/judge0"
	"app/internal/routes"
	"app/internal/s3"
	"app/internal/services"
	"app/internal/stores"
	"log"

	"go.uber.org/fx"
)

func main() {
	if err := boot.LoadEnv(); err != nil {
		log.Fatal(err)
	}

	fx.New(
		fx.Provide(
			boot.NewFirebaseAuth,
			controllers.NewContestController,
			controllers.NewUserController,
			controllers.NewSubmissionController,
			services.NewContestService,
			services.NewUserService,
			services.NewSubmissionService,
			services.NewAdminService,
			internal.NewEchoServer,
			stores.NewStorage,
			db.NewDBConn,
			s3.NewS3Client,
			judge0.NewClient,
		),
		fx.Invoke(routes.AddUserRoutes),
		fx.Invoke(routes.AddContestRoutes),
		fx.Invoke(routes.AddSubmissionRoutes),
		fx.Invoke(routes.AddAdminRoutes),
		fx.Invoke(internal.StartEchoServer),
	).Run()
}
