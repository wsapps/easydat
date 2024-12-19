package cn.easydat.etl.entity.parameter;

public class JobParameterJdbc {

	private String jdbcUrl;
	private String username;
	private String password;
	private String driver;

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	@Override
	public String toString() {
		return "JobParameterJdbc [jdbcUrl=" + jdbcUrl + ", username=" + username + ", password=" + password
				+ ", driver=" + driver + "]";
	}

}
