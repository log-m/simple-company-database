//GROUP 3
//Logan Morris, Troy Kim, Karey Smith, Ashley Handoko
package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.Date;
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
			"INSERT INTO CUSTOMER (firstName, lastName, gender, dob, email)"
			+ "VALUES(?, ?, ?, ?, ?)";
	private static final String retrieveIDSQL = 
			"SELECT * FROM CUSTOMER WHERE id = ?";
	private static final String retrieveZipSQL = 
			"SELECT id, firstName, lastName, gender, dob, email FROM (CUSTOMER join ADDRESS on id = ownerID) WHERE zipcode = ?";
	private static final String retrieveDOBSQL =
			"SELECT * FROM CUSTOMER WHERE dob between ? and ?";
	private static final String updateSQL =
			"UPDATE CUSTOMER SET  firstName = ?, lastName = ?, gender = ?, dob = ?, email = ? WHERE id = ?";
	private static final String  deleteSQL =
			"DELETE FROM CUSTOMER WHERE id = ?";
      
	public Customer create(Connection connection, Customer customer) throws SQLException, DAOException
	{
		if(customer.getId() != null) {
			throw new DAOException("Trying to insert Customer with NON-NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, customer.getFirstName());
			ps.setString(2, customer.getLastName());
			ps.setString(3, customer.getGender().toString());
			ps.setDate(4, customer.getDob());
			ps.setString(5, customer.getEmail());
			
			//execute
			int res = ps.executeUpdate();
			if(res != 1) {
				throw new DAOException("Did Not Create Expected Number Of Rows");
			}
			//assign generated key
			ResultSet krs = ps.getGeneratedKeys();
			krs.next();
			int autoID = krs.getInt(1);
			customer.setId((long) autoID);
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
			//sql
			ps = connection.prepareStatement(retrieveIDSQL);
			
			ps.setLong(1, id);
			
			//execute
			ResultSet rs = ps.executeQuery();
			//check if there is exactly 1 result
			if(!rs.next()) {
				return null;
			}
			rs.last();
			if(rs.getRow() != 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			else {
			//convert row to object
				Customer cust = new Customer();
				cust.setId(rs.getLong("id"));
				cust.setFirstName(rs.getString("firstName"));
				cust.setLastName(rs.getString("lastName"));
				cust.setGender(rs.getString("gender").charAt(0));
				cust.setDob(rs.getDate("dob"));
				cust.setEmail(rs.getString("email"));
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
		if(zipCode == null) {
			throw new DAOException("Trying to retrieve Customer with NULL zip code");
		}
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(retrieveZipSQL);
			ps.setString(1, zipCode);
			//ps.setString(2, cat);
			
			//execute
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			
			rs.beforeFirst();
			List<Customer> custList = new ArrayList<Customer>();
			while(rs.next()){
				//add rows to list
				Customer cust = new Customer();
				cust.setId(rs.getLong("id"));
				cust.setFirstName(rs.getString("firstName"));
				cust.setLastName(rs.getString("lastName"));
				cust.setGender(rs.getString("gender").charAt(0));
				cust.setDob(rs.getDate("dob"));
				cust.setEmail(rs.getString("email"));
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
	
	@Override
	public List<Customer> retrieveByDOB(Connection connection, Date startDate, Date endDate)
			throws SQLException, DAOException{
		
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(retrieveDOBSQL);
			ps.setDate(1, startDate);
			ps.setDate(2, endDate);
			
			//execute
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			rs.beforeFirst();
			List<Customer> custList = new ArrayList<Customer>();
			while(rs.next()) {
				//add rows to list
				Customer cust = new Customer();
				cust.setId(rs.getLong("id"));
				cust.setFirstName(rs.getString("FirstName"));
				cust.setLastName(rs.getString("LastName"));
				cust.setGender(rs.getString("Gender").charAt(0));
				cust.setDob(rs.getDate("Dob"));
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
		if(customer.getId() == null) {
			throw new DAOException("Trying to update Customer with NULL ID");
		}
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(updateSQL);
			ps.setString(1, customer.getFirstName());
			ps.setString(2, customer.getLastName());
			ps.setString(3, customer.getGender().toString());
			ps.setDate(4,customer.getDob());
			ps.setString(5,customer.getEmail());
			ps.setLong(6, customer.getId());
			
			//execute
			int count = ps.executeUpdate();
			
			if(count > 1){
				throw new DAOException("Did Not Update Expected Number Of Rows");
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
			//sql
			ps = connection.prepareStatement(deleteSQL);
			
			ps.setLong(1, id);
			//execute
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
