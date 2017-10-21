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
			"INSERT INTO CREDITCARD (name, ccnumber, expDate, securityCode, ownerID"
			+ "VALUES(?, ?, ?, ?, ?)";
	private static final String retrieveSQL = 
			"SELECT * FROM CREDITCARD WHERE ? = ?";
	private static final String updateSQL =
			"UPDATE CREDITCARD SET name = ?, ccnumber = ?, expDate = ?, securityCode = ? ownerID = ? WHERE ownerID = ?";
	private static final String  deleteSQL =
			"DELETE FROM CREDITCARD WHERE ownerID = ?";
      
CreditCard create(Connection connection, CreditCard creditCard, Long customerID) throws SQLException, DAOException
	{
		if( customerID == null) {
			throw new DAOException("Trying to insert CreditCard with NON-NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL);
			ps.setString(1, creditCard.name());
			ps.setString(2, creditCard.ccnumber());
			ps.setString(3, creditCard.expDate());
			ps.setString(4, creditCard.securityCode());
      ps.setLong(5, customerID);
      
      
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
				CreditCard creditCard = new CreditCard();
				creditCard.setname(rs.getname("name"));
				creditCard.setccnumber(rs.getccnumber("ccnumber"));
				creditCard.setexpDate(rs.getexpDate("expDate"));
        creditCard.setsecurityCode(rs.getsecurityCode("securityCode"));
				return creditCard;
					  
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
