select t.AccountId, acc.ClientId,

       (select round(sum(res_amount), 2) from
           (select
                    cast(replace(amount, ",", ".") as decimal(10,2))*
                    cast(replace(r.Rate, ",", ".") as decimal(10,2)) as res_amount
            from operations as o
                     join rates as r on r.currency = o.currency
            where
                    t.AccountId = AccountDB and t.CutoffDt = DateOp
              and RateDate = (select max(rates.RateDate) from rates)
           )w) as PaymentAmt,

       (select round(sum(res_amount), 2) from
           (select
                    cast(replace(amount, ",", ".") as decimal(10,2))*
                    cast(replace(r.Rate, ",", ".") as decimal(10,2)) as res_amount
            from operations as o
                     join rates as r on r.currency = o.currency
            where
                    t.AccountId = AccountCR and t.CutoffDt = DateOp
              and RateDate = (select max(rates.RateDate) from rates)
           )w) as EnrollmentAmt,

       (select round(sum(res_amount), 2) from
           (select
                    cast(replace(amount, ",", ".") as decimal(10,2))*
                    cast(replace(r.Rate, ",", ".") as decimal(10,2)) as res_amount
            from operations as o
                     join rates as r on r.currency = o.currency
            where
                    t.AccountId = AccountDB and t.CutoffDt = DateOp
              and substring(acc.AccountNum, 0, 5) = "40702"
              and RateDate = (select max(rates.RateDate) from rates)
           )w) as TaxAmt,

       (select round(sum(res_amount), 2) from
           (select
                    cast(replace(amount, ",", ".") as decimal(10,2))*
                    cast(replace(r.Rate, ",", ".") as decimal(10,2)) as res_amount
            from operations as o
                     join rates as r on r.currency = o.currency
            where
                    t.AccountId = AccountCR and t.CutoffDt = DateOp
              and substring(acc.AccountNum, 0, 5) = "40802"
              and RateDate = (select max(rates.RateDate) from rates)
           )w) as ClearAmt,

       (select round(sum(res_amount), 2) from
           (select distinct amount,
                            cast(replace(amount, ",", ".") as decimal(10,2))*
                            cast(replace(r.Rate, ",", ".") as decimal(10,2)) as res_amount
            from operations as o
                     join tech_params on o.comment not like concat("%",list_1,"%")
                     join rates as r on r.currency = o.currency
            where
                    t.AccountId = AccountDB and t.CutoffDt = DateOp
              and RateDate = (select max(rates.RateDate) from rates)
              and list_1 is not null
           )w) as CarsAmt,

       (select round(sum(res_amount), 2) from
           (select distinct amount,
                            cast(replace(amount, ",", ".") as decimal(10,2))*
                            cast(replace(r.Rate, ",", ".") as decimal(10,2)) as res_amount
            from operations as o
                     join tech_params on o.comment like concat("%",list_2,"%")
                     join rates as r on r.currency = o.currency
            where
                    t.AccountId = AccountCR and t.CutoffDt = DateOp
              and RateDate = (select max(rates.RateDate) from rates)
              and list_2 is not null
           )w) as FoodAmt,

       (select round(sum(res_amount), 2) from
           (select
                    cast(replace(amount, ",", ".") as decimal(10,2))*
                    cast(replace(r.Rate, ",", ".") as decimal(10,2)) as res_amount
            from operations as o
                     join rates as r on r.currency = o.currency
                     join accounts as a on o.AccountCR = a.AccountID
                     join clients as c on a.ClientId = c.ClientID
            where
                    c.type = "Ф" and
                    t.AccountId = AccountDB and t.CutoffDt = DateOp
              and RateDate = (select max(rates.RateDate) from rates)
           )w) as FLAmt,

       t.CutoffDt
from(
        select distinct AccountDB as AccountId, DateOp as CutoffDt from operations
        union
        select distinct AccountCR as AccountId, DateOp as CutoffDt from operations) t
        join accounts as acc on acc.AccountId = t.AccountId