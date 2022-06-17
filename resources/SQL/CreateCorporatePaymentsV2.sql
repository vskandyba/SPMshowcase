with kernel_id as
(
    select t.AccountId, ClientId, CutoffDt
    from (
             select distinct AccountDB as AccountId, DateOp as CutoffDt
             from operations
             union
             select distinct AccountCR as AccountId, DateOp as CutoffDt
             from operations
         ) t
    join accounts as a on t.AccountId = a.AccountID
),
temp_operations as
(
    select AccountDB,
           AccountCR,
           DateOp,
           round(
           cast(replace(amount, ",", ".") as decimal(10,2)) *
           cast(replace(r.Rate, ",", ".") as decimal(10,2)), 2) as res_amount,
           Comment
    from operations as o
    join rates as r on o.Currency = r.Currency
    where RateDate = (select max(rates.RateDate) from rates)
),
RawCarsAmt as
(
    select *
    from temp_operations
        except -- дорогая операция
    select distinct AccountDB,
                    AccountCR,
                    DateOp,
                    res_amount,
                    Comment
    from temp_operations
             join tech_params on Comment like concat("% ", list_1, ",%") -- спастить от вин (свин)
),
RawFoodAmt as
(
    select distinct AccountDB,
                    AccountCR,
                    DateOp,
                    res_amount
    from temp_operations
             join tech_params on Comment like concat("% ",list_2,",%") -- спастить от кофе (кофемаш)
)


select AccountId,
       ClientId,
       coalesce((select sum(res_amount) from temp_operations
                 where AccountDB = AccountId and DateOp = CutoffDt
                ), 0) as PaymentAmt,
       coalesce((select sum(res_amount) from temp_operations
                 where AccountCR = AccountId and DateOp = CutoffDt
                ), 0) as EnrollmentAmt,
       coalesce((select sum(res_amount) from temp_operations
                 join accounts as acc on AccountCR = acc.AccountId
                 where AccountDB = kernel_id.AccountId and DateOp = CutoffDt
                   and substring(acc.AccountNum, 0, 5) = "40702"
                ), 0) as TaxAmt,
       coalesce((select sum(res_amount) from temp_operations
                 join accounts as acc on AccountDB = acc.AccountId
                 where AccountCR = kernel_id.AccountId and DateOp = CutoffDt
                   and substring(acc.AccountNum, 0, 5) = "40802"
                ), 0) as ClearAmt,
       coalesce((select sum(res_amount) from RawCarsAmt
                 where AccountDB = AccountId and DateOp = CutoffDt
                ), 0) as CarsAmt,
       coalesce((select sum(res_amount) from RawFoodAmt
                 where AccountCR = AccountId and DateOp = CutoffDt
                ), 0) as FoodAmt,
       coalesce((select sum(res_amount) from temp_operations
                 join accounts as a on AccountCR = a.AccountID
                 join clients as c on a.ClientId = c.ClientID
                 where AccountDB = kernel_id.AccountId and DateOp = CutoffDt and c.type = "Ф"
                ), 0) as FLAmt,
       CutoffDt
from kernel_id
--order by cast(AccountId as integer)