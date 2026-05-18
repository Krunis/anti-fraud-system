package fraud

func (a *AntiFraud) sendInClickHouse() error{
	a.clickHouse.Conn.Query()
}