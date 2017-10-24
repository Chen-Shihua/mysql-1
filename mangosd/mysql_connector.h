#ifndef PSL_MYSQL_CONNECTOR_H_
#define PSL_MYSQL_CONNECTOR_H_

#include <mysql.h>
#include <string>
#include <map>

class ResultSet
{
public:
	ResultSet(MYSQL_RES * pres);
	ResultSet(const ResultSet & other) = delete;
	ResultSet & operator = (const ResultSet & other) = delete;
	ResultSet(ResultSet && other);
	~ResultSet();
	bool next();
	const char * getString(const char * key);
	operator bool();
	int getInt(const char * key);
	double getDouble(const char * key);
	long long getLongLong(const char * key);
	long getLong(const char * key);
protected:
	bool getKeyIndex(const char * key, unsigned int & index);
private:
	MYSQL_RES * m_res;
	MYSQL_ROW m_curRow;
	std::map<std::string, unsigned int> m_keyMap;
};

class mysql_connector
{
public:
    mysql_connector(const char * host, const char * user, const char * passwd, const char *db, unsigned int port);
    ~mysql_connector();
	bool execute(const char * format, ...);
	bool executeUpdate(my_ulonglong & updateRow, const char * format, ...);
	ResultSet executeQuery(const char * format, ...);
	my_ulonglong getInsertId();
protected:
    bool connect();
private:
	MYSQL * m_pmysql;
	std::string m_ip;
	std::string m_username;
	std::string m_password;
	std::string m_db;
	unsigned int m_port;
	time_t m_lasttime;
};
#endif // PSL_MYSQL_CONNECTOR_H_

