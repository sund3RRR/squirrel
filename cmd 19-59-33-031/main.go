package main

import (
	"fmt"
	"log"

	"github.com/Masterminds/squirrel"
)

func main() {
	builder := squirrel.
		Merge("orders AS o").
		ValuesAlias("vals").
		Columns("order_id", "currency", "seller", "update_ts", "create_ts").
		On("o.order_id = vals.order_id").
		When(`
			MATCHED THEN
				UPDATE SET
					update_ts    = vals.update_ts,
		`).
		When(`
			NOT MATCHED THEN
				INSERT (
					order_id,
					currency,
					seller,
					brand_name,
					update_ts,
					create_ts
				)
				VALUES (
					vals.order_id,
					vals.currency,
					vals.seller,
					vals.brand_name,
					vals.update_ts,
					vals.create_ts
				)
		`).
		Suffix("RETURNING merge_action(), o.*").
		PlaceholderFormat(squirrel.Dollar)

	values := [][]interface{}{
		{1, "USD", "Amazon", "Adidas", "2020-01-01 00:00:00", "2020-01-01 00:00:00"},
		{2, "USD", "Amazon", "Nike", "2020-01-01 00:00:00", "2020-01-01 00:00:00"},
	}

	for _, v := range values {
		builder = builder.Values(v...)
	}

	sql, args, err := builder.ToSql()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(sql)
	fmt.Println(args)

	b := squirrel.Merge("a").
		Values(1, 2).
		ValuesAlias("vals").
		Columns("b", "c").
		On("a.b = vals.b AND a.c = vals.c").
		When("MATCHED THEN UPDATE SET b = vals.b, c = vals.c").
		When("NOT MATCHED THEN INSERT (b, c) VALUES (vals.b, vals.c)").
		Suffix("RETURNING ?", 5)

	sql, args, err = b.ToSql()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(sql)
	fmt.Println(args)
}
