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
	binit_mysql_ = false;
	bconnect_ = false;
	if (NULL != host)
	{
		host_.assign(host);
	}
	if (NULL != user)
	{
		user_.assign(user);
	}
	if (NULL != passwd)
	{
		passwd_.assign(passwd);
	}
	if (NULL != db)
	{
		db_.assign(db);
	}
	port_ = port;
	time(&m_lasttime);
}

mysql_connector::~mysql_connector()
{
	if (binit_mysql_)
	{
		mysql_close(&mysql_);
	}
}

bool mysql_connector::connect()
{
	time_t now;
	time(&now);
	if (now - m_lasttime > 1800)
	{
		bconnect_ = false;
	}
	m_lasttime = now;
	if (bconnect_)
	{
		return true;
	}
	if (binit_mysql_)
	{
		mysql_close(&mysql_);
		binit_mysql_ = false;
	}
	if (!mysql_init(&mysql_))
	{
		return false;
	}
	binit_mysql_ = true;
	if (mysql_options(&mysql_, MYSQL_SET_CHARSET_NAME, "utf8"))
	{
		return false;
	}
	my_bool breconnect = false;
	if (mysql_options(&mysql_, MYSQL_OPT_RECONNECT, &breconnect))
	{
		return false;
	}
	if (NULL == mysql_real_connect(&mysql_, host_.c_str(), user_.c_str(), passwd_.c_str(), db_.c_str(), port_, NULL, CLIENT_FOUND_ROWS))
	{
		MYLOG(g_log, LOGERROR)(mysql_errno(&mysql_))(mysql_error(&mysql_));
		return false;
	}
	if (mysql_autocommit(&mysql_, 1))
	{
		return false;
	}
	bconnect_ = true;
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
	int iquery = mysql_query(&mysql_, querySql);
	if (iquery != 0)
	{
		bconnect_ = false;
		MYLOG(g_log, LOGERROR)(mysql_error(&mysql_))(querySql);
		return false;
	}
	MYSQL_RES * pres = mysql_store_result(&mysql_);
	if (pres != nullptr)
	{
		mysql_free_result(pres);
		return true;
	}
	else
	{
		unsigned int fields = mysql_field_count(&mysql_);
		if (fields != 0)
		{
			bconnect_ = false;
			MYLOG(g_log, LOGERROR)(mysql_error(&mysql_))(querySql);
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
	int iquery = mysql_query(&mysql_, querySql);
	if (iquery != 0)
	{
		bconnect_ = false;
		MYLOG(g_log, LOGERROR)(mysql_error(&mysql_))(querySql);
		return false;
	}
	MYSQL_RES * pres = mysql_store_result(&mysql_);
	if (pres != nullptr)
	{
		mysql_free_result(pres);
		MYLOG(g_log, LOGERROR)(querySql);
		return false;
	}
	else
	{
		unsigned int fields = mysql_field_count(&mysql_);
		if (fields != 0)
		{
			bconnect_ = false;
			MYLOG(g_log, LOGERROR)(mysql_error(&mysql_))(querySql);
			return false;
		}
		else
		{
			updateRow = mysql_affected_rows(&mysql_);
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
	int iquery = mysql_query(&mysql_, querySql);
	if (iquery != 0)
	{
		bconnect_ = false;
		MYLOG(g_log, LOGERROR)(mysql_error(&mysql_))(querySql);
		return ResultSet(nullptr);
	}
	MYSQL_RES * pres = mysql_store_result(&mysql_);
	if (pres != nullptr)
	{
		return ResultSet(pres);
	}
	else
	{
		unsigned int fields = mysql_field_count(&mysql_);
		if (fields != 0)
		{
			bconnect_ = false;
			MYLOG(g_log, LOGERROR)(mysql_error(&mysql_))(querySql);
		}
		else
		{
			MYLOG(g_log, LOGERROR)("not a query statement")(querySql);
		}
		return ResultSet(nullptr);
	}
}

my_ulonglong mysql_connector::getInsertId()
{
	return mysql_insert_id(&mysql_);
}
