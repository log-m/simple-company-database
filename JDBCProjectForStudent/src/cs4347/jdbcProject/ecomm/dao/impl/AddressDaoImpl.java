//GROUP 3
//Logan Morris, Troy Kim, Karey Smith, Ashley Handoko
package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class AddressDaoImpl implements AddressDAO
{
	private static final String insertSQL = 
			"INSERT INTO ADDRESS (address1, address2, city, state, zipcode, ownerID)"
			+ "VALUES(?, ?, ?, ?, ?, ?)";
	private static final String retrieveSQL = 
			"SELECT * FROM ADDRESS WHERE ownerID = ?";
	//private static final String updateSQL =
		//	"UPDATE ADDRESS SET address1 = ?, address2 = ?, city = ?, state = ? zipcode = ? WHERE ownerID = ?";
	private static final String  deleteSQL =
			"DELETE FROM ADDRESS WHERE ownerID = ?";
      
	public Address create(Connection connection, Address address, Long customerID ) throws SQLException, DAOException
	{
		if( customerID == null) {
			throw new DAOException("Trying to insert Address with NON-NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(insertSQL);
			ps.setString(1, address.getAddress1());
			ps.setString(2, address.getAddress2());
			ps.setString(3, address.getCity());
			ps.setString(4, address.getState());
			ps.setString(5, address.getZipcode());
   		   	ps.setLong(6, customerID);
      
   		   	//execute
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
			//sql
			ps = connection.prepareStatement(retrieveSQL);
			//ps.setString(1, "ownerID");
			ps.setLong(1, customerID);
			
			//execute
			ResultSet rs = ps.executeQuery();
			
			//check if exactly 1 result
			if(!rs.next()) {
				return null;
			}
			rs.last();
			if(rs.getRow() != 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			else {
				//convert row to object
				Address address = new Address();
				address.setAddress1(rs.getString("address1"));
				address.setAddress2(rs.getString("address2"));
				address.setCity(rs.getString("city"));
        address.setState(rs.getString("state"));
        address.setZipcode(rs.getString("zipcode"));
       
        
        
				return address;
					  
			}
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
public	void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException{
  if(customerID == null) {
			throw new DAOException("Trying to delete address with NULL customer ID");
		}
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(deleteSQL);
			
			ps.setLong(1, customerID);
			//execute
			int count = ps.executeUpdate();
			
			if(count > 1) {
					  throw new DAOException("Did Not Delete Expected Number Of Rows");
			}
			//return count;
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
}

