select cp.AccountId, acc.AccountNum, acc.DateOpen,
       cp.ClientId, c.ClientName,
       cast(coalesce(cp.PaymentAmt, 0) as decimal(10,2)) +
       cast(coalesce(cp.EnrollmentAmt, 0) as decimal(10,2)) as TotalAmt,
       cp.CutoffDt from corporate_payments as cp
                            join accounts as acc on cp.AccountId = acc.AccountId
                            join clients as c on cp.ClientId = c.ClientId