package main

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"fmt"
	"os"
	"errors"
	"flag"
	"github.com/fatih/color"
)

type Config struct {
	Username string `consul:"default:root"`
	Password string `consul:"default:keepitsimple"`
	Host     string `consul:"default:localhost"`
	Port     int    `consul:"default:5432"`
}

type Schema struct {
	DbName string
	Tables []string
}

type DbDriver struct {
	dbMap map[string]*gorm.DB
}

func (d *DbDriver) Init(cfg *Config, dbs ...Schema) {
	d.dbMap = make(map[string]*gorm.DB)

	for _, schema := range dbs {
		db, err := gorm.Open(
			"postgres",
			fmt.Sprintf(
				"postgres://%s:%s@%s:%d/%s?sslmode=disable",
				cfg.Username,
				cfg.Password,
				cfg.Host,
				cfg.Port,
				schema.DbName,
			),
		)

		d.dbMap[schema.DbName] = db

		if err != nil {
			fmt.Println(schema, err)
			os.Exit(1)
		}
	}
}

func (d *DbDriver) Close() {
	for _, dbConn := range d.dbMap {
		dbConn.Close()
	}
}

func (d *DbDriver) Get(db string) (*gorm.DB, error) {
	v, ok := d.dbMap[db]
	if !ok {
		return nil, errors.New("could not get db")
	}
	return v, nil
}

var (
	flagUsername = flag.String("u", "", "Username")
	flagPassword = flag.String("up", "", "Password")
	flagPort = flag.Int("p", 0, "Port")
	flagHost = flag.String("h", "", "Host")
	flagPostfix = flag.String("postfix", "", "Postrfix")
)

func init() {
	flag.Parse()
}

func main() {
	if *flagUsername == "" || *flagPassword == "" || *flagPort == 0 || *flagHost == "" {
		flag.Usage()
		os.Exit(0)
	}

	c := &Config{
		Username: *flagUsername,
		Password: *flagPassword,
		Port: *flagPort,
		Host: *flagHost,
	}

	dbs := []Schema{
		{
			DbName: "archisyncsvc" + *flagPostfix,
			Tables: []string{
				"cursors",
				"deliveries",
				"relations",
			},
		},
		{
			DbName: "claimsvc" + *flagPostfix,
			Tables: []string{
				"claims",
				"change_logs",
			},
		},
		{
			DbName: "clientsvc" + *flagPostfix,
			Tables: []string{
				"addresses",
				"change_logs",
				"client_infos",
				"clients",
				"contacts",
				"passports",
				"phones",
				"socials",
			},
		},
		{
			DbName: "loansvc" + *flagPostfix,
			Tables: []string{
				"loans",
				"change_logs",
			},
		},
		{
			DbName: "prolongationsvc" + *flagPostfix,
			Tables: []string{
				"prolongations",
				"change_logs",
			},
		},
		{
			DbName: "cashsvc" + *flagPostfix,
			Tables: []string{
				"cash_box_states",
				"transactions",
				"change_logs",
			},
		},
		{
			DbName: "noncashsvc" + *flagPostfix,
			Tables: []string{
				"transactions",
				"change_logs",
			},
		},
		{
			DbName: "loantransactionsvc" + *flagPostfix,
			Tables: []string{
				"loan_transactions",
			},
		},
		{
			DbName: "assign",
			Tables: []string{
				"assignment",
				"event_journal",
			},
		},
	}

	dbDriver := DbDriver{}
	dbDriver.Init(c, dbs...)

	fmt.Println("connections established successfully")

	for _, schema := range dbs {
		fmt.Println("clean db", schema.DbName)
		conn, err := dbDriver.Get(schema.DbName)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		for _, tableName := range schema.Tables {
			fmt.Println("clean table", tableName)
			err = conn.Exec(fmt.Sprintf("DELETE FROM %s", tableName)).Error
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	defer dbDriver.Close()
	color.New(color.FgGreen).Add(color.Underline).Println("cleaned successfully")
}
