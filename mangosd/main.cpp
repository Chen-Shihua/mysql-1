#include <iostream>
#include "global.h"
#include "mysql_connector.h"

MyLog g_log;

int main(int argc, char * argv[])
{
	g_log.init("log", nullptr, "./", 0);
	mysql_connector connector("192.168.31.231", "root", "123456", "lb_livegame", 3306);
	ResultSet rs1 = connector.executeQuery("update lb_user set money = 1000 where uid = 2");
	if (rs1)
	{
		while (rs1.next())
		{
			std::cout << rs1.getString("user_name") << std::endl;
		}
	}
	return 0;
}