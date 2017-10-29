package cs4347.jdbcProject.ecomm.dao.impl;

import cs4347.jdbcProject.ecomm.dao.CustomerDAO;

public class CustomerDaoImpl implements CustomerDAO
{
	
}
package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerDaoImpl implements CustomerDAO
{
	private static final String insertSQL = 
			"INSERT INTO ADDRESS (id, firstName, lastName, gender, dob, email"
			+ "VALUES(?, ?, ?, ?, ?, ?)";
	private static final String retrieveSQL = 
			"SELECT * FROM CUSTOMER WHERE ? = ?";
	private static final String updateSQL =
			"UPDATE CUSTOMER SET"  firstName = ?, lastName = ?, gender = ?, dob = ?, email = ? WHERE id = ?;
	private static final String  deleteSQL =
			"DELETE FROM CUSTOMER WHERE id = ?";
      
	public Customer create(Connection connection, Customer customer) throws SQLException, DAOException
	{
		if(product.getId() != null) {
			throw new DAOException("Trying to insert Customer with NON-NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, Customer.firstName());
			ps.setString(2, Customer.lastName());
			ps.setString(3, Customer.gender());
			ps.setString(4, Customer.dob());
			ps.setString(5, Customer.email());
			
			
			int res = ps.executeUpdate();
			if(res != 1) {
				throw new DAOException("Did Not Create Expected Number Of Rows");
			}
			ResultSet krs = ps.getGeneratedKeys();
			krs.next();
			int autoID = krs.getInt(1);
			product.setId((long) autoID);
			return customer;
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public Customer retrieve(Connection connection, Long id) throws SQLException, DAOException{
		if(id == null) {
			throw new DAOException("Trying to retrieve Customer with NULL ID");
		}
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retrieveSQL);
			ps.setString(1, "id");
			ps.setLong(2, id);
			
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			rs.last();
			if(rs.getRow() != 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			else {
			
				Customer cust = new Customer();
				cust.setId(rs.getLong("id"));
				cust.setFirstName(rs.getString("FirstName"));
				cust.setLastName(rs.getString("LastName"));
				cust.setGender(rs.getInt("Gender"));
				cust.setDob(rs.getString("Dob"));
				cust.setEmail(rs.getString("Email"));
				return cust;
					  
			}
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public List<Customer> retrieveByZipCode(Connection connection, String zipCode) throws SQLException, DAOException{
		if(Address == null) {
			throw new DAOException("Trying to retrieve Product with NULL Address code");
		}
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retrieveSQL);
			ps.setString(1, "custAddress");
			ps.setString(2, cat);
			
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			rs.last();
			if(rs.getRow() != 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			else {
				cust = new Customer();
				cust.setId(rs.getLong("id"));
				cust.setFirstName(rs.getString("FirstName"));
				cust.setLastName(rs.getString("LastName"));
				cust.setGender(rs.getInt("Gender"));
				cust.setDob(rs.getString("Dob"));
				cust.setEmail(rs.getString("Email"));
				custList.add(cust);
					  
			}
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public List<Customer> retrieveByDOB(Connection connection, Date startDate, Date endDate) throws SQLException, DAOException{
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retrieveSQL);
			ps.setString(1, "dob");
			ps.setInt(2, DATE);
			
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			rs.beforeFirst();
			List<Product> prodList = new ArrayList<Customer>();
			while(rs.next()) {
				Product cust = new Customer();
				cust.setId(rs.getLong("id"));
				cust.setFirstName(rs.getString("FirstName"));
				cust.setLastName(rs.getString("LastName"));
				cust.setGender(rs.getInt("Gender"));
				cust.setDob(rs.getString("Dob"));
				cust.setEmail(rs.getString("Email"));
				custList.add(cust);
					  
			}
			return custList;
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	
	public int update(Connection connection, Customer customer) throws SQLException, DAOException{
		if(id == null) {
			throw new DAOException("Trying to update Customer with NULL ID");
		}
		PreparedStatement ps = null;
		try {

			ps = connection.prepareStatement(updateSQL);
			ps.setString(1, "firstName");
			ps.setString(2, "lastName");
			ps.setString(3, "gender");
			ps.setString(4,"dob");
			ps.setString(5,"email");
			ps.setLong(6, id);
			
			int count = ps.executeUpdate();
			
			if(count > 1){
				throw new DAOException("Did Not Update Expected Number Of Rows);
			}
			return count;
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public int delete(Connection connection, Long id) throws SQLException, DAOException{
		if(id == null) {
			throw new DAOException("Trying to delete Customer with NULL ID");
		}
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(deleteSQL);
			
			ps.setLong(1, id);
			
			int count = ps.executeUpdate();
			
			if(count > 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			return count;
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
}
