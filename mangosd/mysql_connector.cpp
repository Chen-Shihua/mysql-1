#include <assert.h>
#include "mysql_connector.h"
#include "global.h"

ResultSet::ResultSet(MYSQL_RES * pres)
{
	m_curRow = nullptr;
	m_res = pres;
	if (m_res)
	{
		unsigned int num = mysql_num_fields(m_res);
		MYSQL_FIELD * fields = mysql_fetch_fields(m_res);
		for (unsigned int i = 0; i < num; ++i)
		{
			m_keyMap.insert(std::pair<std::string, unsigned int>(fields[i].name, i));
		}
	}
}

ResultSet::ResultSet(ResultSet && other)
{
	m_res = other.m_res;
	other.m_res = nullptr;
	m_curRow = other.m_curRow;
	other.m_curRow = nullptr;
	m_keyMap.swap(other.m_keyMap);
}

ResultSet::~ResultSet()
{
	if (m_res)
	{
		mysql_free_result(m_res);
	}
}

bool ResultSet::next()
{
	assert(nullptr != m_res);
	m_curRow = mysql_fetch_row(m_res);
	if (nullptr != m_curRow)
	{
		return true;
	}
	else
	{
		return false;
	}
}

const char * ResultSet::getString(const char * key)
{
	unsigned int index;
	if (!getKeyIndex(key, index))
	{
		return nullptr;
	}
	return m_curRow[index];
}

ResultSet::operator bool()
{
	return m_res != nullptr;
}

int ResultSet::getInt(const char * key)
{
	const char * szValue = getString(key);
	if (nullptr == szValue)
	{
		return 0;
	}
	return atoi(szValue);
}

double ResultSet::getDouble(const char * key)
{
	const char * szValue = getString(key);
	if (nullptr == szValue)
	{
		return 0.0;
	}
	return atof(szValue);
}

long long ResultSet::getLongLong(const char * key)
{
	const char * szValue = getString(key);
	if (nullptr == szValue)
	{
		return 0LL;
	}
	return atoll(szValue);
}

long ResultSet::getLong(const char * key)
{
	const char * szValue = getString(key);
	if (nullptr == szValue)
	{
		return 0L;
	}
	return atol(szValue);
}

bool ResultSet::getKeyIndex(const char * key, unsigned int & index)
{
	auto iter = m_keyMap.find(key);
	if (iter == m_keyMap.end())
	{
		return false;
	}
	else
	{
		index = iter->second;
		return true;
	}
}

mysql_connector::mysql_connector(const char *host, const char *user, const char *passwd, const char *db, unsigned int port)
{
	m_pmysql = nullptr;
	m_lasttime = 0;
	if (NULL != host)
	{
		m_ip.assign(host);
	}
	if (NULL != user)
	{
		m_username.assign(user);
	}
	if (NULL != passwd)
	{
		m_password.assign(passwd);
	}
	if (NULL != db)
	{
		m_db.assign(db);
	}
	m_port = port;
}

mysql_connector::~mysql_connector()
{
	if (m_pmysql)
	{
		mysql_close(m_pmysql);
	}
}

bool mysql_connector::connect()
{
	time_t now;
	time(&now);
	if (now - m_lasttime > 1800)
	{
		if (m_pmysql)
		{
			mysql_close(m_pmysql);
			m_pmysql = nullptr;
		}
	}
	if (m_pmysql)
	{
		m_lasttime = now;
		return true;
	}
	if (now - m_lasttime < 5)
	{
		return false;
	}
	m_lasttime = now;
	m_pmysql = mysql_init(nullptr);
	if (nullptr == m_pmysql)
	{
		return false;
	}
	if (mysql_options(m_pmysql, MYSQL_SET_CHARSET_NAME, "utf8"))
	{
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return false;
	}
	my_bool breconnect = false;
	if (mysql_options(m_pmysql, MYSQL_OPT_RECONNECT, &breconnect))
	{
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return false;
	}
	if (NULL == mysql_real_connect(m_pmysql, m_ip.c_str(), m_username.c_str(), m_password.c_str(), m_db.c_str(), m_port, NULL, CLIENT_FOUND_ROWS))
	{
		MYLOG(g_log, LOGERROR)(mysql_errno(m_pmysql))(mysql_error(m_pmysql));
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return false;
	}
	if (mysql_autocommit(m_pmysql, 1))
	{
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return false;
	}
	return true;
}


bool mysql_connector::execute(const char * format, ...)
{
	if (!connect())
	{
		return false;
	}
	va_list ap;
	char querySql[1024 * 2];
	va_start(ap, format);
	vsnprintf(querySql, sizeof(querySql), format, ap);
	va_end(ap);
	int iquery = mysql_query(m_pmysql, querySql);
	if (iquery != 0)
	{
		MYLOG(g_log, LOGERROR)(mysql_error(m_pmysql))(querySql);
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return false;
	}
	MYSQL_RES * pres = mysql_store_result(m_pmysql);
	if (pres != nullptr)
	{
		mysql_free_result(pres);
		return true;
	}
	else
	{
		unsigned int fields = mysql_field_count(m_pmysql);
		if (fields != 0)
		{
			MYLOG(g_log, LOGERROR)(mysql_error(m_pmysql))(querySql);
			mysql_close(m_pmysql);
			m_pmysql = nullptr;
			return false;
		}
		else
		{
			return true;
		}
	}
}

bool mysql_connector::executeUpdate(my_ulonglong & updateRow, const char * format, ...)
{
	if (!connect())
	{
		return false;
	}
	va_list ap;
	char querySql[1024 * 2];
	va_start(ap, format);
	vsnprintf(querySql, sizeof(querySql), format, ap);
	va_end(ap);
	int iquery = mysql_query(m_pmysql, querySql);
	if (iquery != 0)
	{
		MYLOG(g_log, LOGERROR)(mysql_error(m_pmysql))(querySql);
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return false;
	}
	MYSQL_RES * pres = mysql_store_result(m_pmysql);
	if (pres != nullptr)
	{
		mysql_free_result(pres);
		MYLOG(g_log, LOGERROR)("not a update statement")(querySql);
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return false;
	}
	else
	{
		unsigned int fields = mysql_field_count(m_pmysql);
		if (fields != 0)
		{
			MYLOG(g_log, LOGERROR)(mysql_error(m_pmysql))("not a update statement")(querySql);
			mysql_close(m_pmysql);
			m_pmysql = nullptr;
			return false;
		}
		else
		{
			updateRow = mysql_affected_rows(m_pmysql);
			return true;
		}
	}
}

ResultSet mysql_connector::executeQuery(const char * format, ...)
{
	if (!connect())
	{
		return ResultSet(nullptr);
	}
	va_list ap;
	char querySql[1024 * 2];
	va_start(ap, format);
	vsnprintf(querySql, sizeof(querySql), format, ap);
	va_end(ap);
	int iquery = mysql_query(m_pmysql, querySql);
	if (iquery != 0)
	{
		MYLOG(g_log, LOGERROR)(mysql_error(m_pmysql))(querySql);
		mysql_close(m_pmysql);
		m_pmysql = nullptr;
		return ResultSet(nullptr);
	}
	MYSQL_RES * pres = mysql_store_result(m_pmysql);
	if (pres != nullptr)
	{
		return ResultSet(pres);
	}
	else
	{
		unsigned int fields = mysql_field_count(m_pmysql);
		if (fields != 0)
		{
			MYLOG(g_log, LOGERROR)(mysql_error(m_pmysql))(querySql);
			mysql_close(m_pmysql);
			m_pmysql = nullptr;
		}
		else
		{
			MYLOG(g_log, LOGERROR)("not a query statement")(querySql);
			mysql_close(m_pmysql);
			m_pmysql = nullptr;
		}
		return ResultSet(nullptr);
	}
}

my_ulonglong mysql_connector::getInsertId()
{
	return mysql_insert_id(m_pmysql);
}
