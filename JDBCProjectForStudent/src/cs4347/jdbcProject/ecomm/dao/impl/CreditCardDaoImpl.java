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

import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CreditCardDaoImpl implements CreditCardDAO
{
	private static final String insertSQL = 
			"INSERT INTO CREDITCARD (name, ccNumber, expDate, securityCode, ownerID)"
			+ "VALUES(?, ?, ?, ?, ?)";
	private static final String retrieveSQL = 
			"SELECT * FROM CREDITCARD WHERE ownerID = ?";
	//private static final String updateSQL =
			//"UPDATE CREDITCARD SET name = ?, ccnumber = ?, expDate = ?, securityCode = ? ownerID = ? WHERE ownerID = ?";
	private static final String  deleteSQL =
			"DELETE FROM CREDITCARD WHERE ownerID = ?";
      
public CreditCard create(Connection connection, CreditCard creditCard, Long customerID) throws SQLException, DAOException
	{
		if( customerID == null) {
			throw new DAOException("Trying to insert CreditCard with NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(insertSQL);
			ps.setString(1, creditCard.getName());
			ps.setString(2, creditCard.getCcNumber());
			ps.setString(3, creditCard.getExpDate());
			ps.setString(4, creditCard.getSecurityCode());
      ps.setLong(5, customerID);
      
      	//execute
			int res = ps.executeUpdate();
			if(res != 1) {
				throw new DAOException("Did Not Create Expected Number Of Rows");
			}
		
			return creditCard;
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public  CreditCard retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException{
		if(customerID == null) {
			throw new DAOException("Trying to retrieve Credit Card with NULL customer ID");
		}
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(retrieveSQL);
			//ps.setString(1, "ownerID");
			ps.setLong(1, customerID);
			
			//execute
			ResultSet rs = ps.executeQuery();
			//check if exactly one result
			if(!rs.next()) {
				return null;
			}
			rs.last();
			if(rs.getRow() != 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			else {
				//convert row to object
				CreditCard creditCard = new CreditCard();
				creditCard.setName(rs.getString("name"));
				creditCard.setCcNumber(rs.getString("ccNumber"));
				creditCard.setExpDate(rs.getString("expDate"));
        creditCard.setSecurityCode(rs.getString("securityCode"));
				return creditCard;
					  
			}
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException{
  if(customerID == null) {
			throw new DAOException("Trying to delete Credit Card with NULL customer ID");
		}
		PreparedStatement ps = null;
		try {
			//sql
			ps = connection.prepareStatement(deleteSQL);
			
			ps.setLong(1, customerID);
			//execute
			int count = ps.executeUpdate();
			
			if(count > 1) {
					  throw new DAOException("Did Not delete Expected Number Of Rows");
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

