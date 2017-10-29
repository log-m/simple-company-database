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
			"UPDATE CUSTOMER SET  WHERE id = ?, firstName = ?, lastName = ?, gender = ? dob = ? email = ?";
	private static final String  deleteSQL =
			"DELETE FROM CUSTOMER WHERE id = ?";
      
	public Address create(Connection connection, Address address, Long customerID ) throws SQLException, DAOException
	{
		if( customerID == null) {
			throw new DAOException("Trying to insert Address with NON-NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL);
			ps.setString(1, address.getaddress1());
			ps.setString(2, address.getaddress2());
			ps.setString(3, address.getcity());
			ps.setString(4, address.getstate());
			ps.setString(5, address.getzipcode());
   		   	ps.setLong(6, customerID);
      
      
			int res = ps.executeUpdate();
			if(res != 1) {
				throw new DAOException("Did Not Create Expected Number Of Rows");
			}
		
			return address;
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public  Address retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException{
		if(customerID == null) {
			throw new DAOException("Trying to retrieve Address with NULL ID");
		}
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retrieveSQL);
			ps.setString(1, "ownerID");
			ps.setLong(2, customerID);
			
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			rs.last();
			if(rs.getRow() != 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			else {
				Address address = new Address();
				address.setaddress1(rs.getaddress1("address1"));
				address.setaddress2(rs.getaddress2("address2"));
				address.setcity(rs.getcity("city"));
        address.setstate(rs.getstate("state));
        address.setzipcode(rs.getzipcode("zipcode"));
       
        
        
				return address;
					  
			}
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException{
  if(customerID == null) {
			throw new DAOException("Trying to delete customerID with NULL ID");
		}
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(deleteSQL);
			
			ps.setLong(1, customerID);
			
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
}
