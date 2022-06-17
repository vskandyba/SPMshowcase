select c.ClientId, c.ClientName, c.Type, c.Form, c.RegisterDate,
       sum(ca.TotalAmt) as TotalAmt,
       ca.CutoffDt from clients as c
join corporate_account as ca on c.ClientId = ca.ClientId
group by c.ClientId, c.ClientName, c.Type, c.Form, c.RegisterDate, ca.CutoffDt