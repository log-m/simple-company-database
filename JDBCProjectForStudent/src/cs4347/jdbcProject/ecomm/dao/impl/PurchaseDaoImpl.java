//GROUP 3
//Logan Morris, Troy Kim, Karey Smith, Ashley Handoko
package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;

import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class PurchaseDaoImpl implements PurchaseDAO
{
	private static final String insertSQL = "INSERT INTO PURCHASE (productID, customerID, purchaseDate, purchaseAmount)"
			+ "VALUES(?, ?, ?, ?)";
	private static final String retrieveSQL = 
			"SELECT * FROM PURCHASE WHERE id = ?";
	private static final String retrieveCustSQL = 
			"SELECT * FROM PURCHASE WHERE customerID = ?";
	private static final String retrieveProdSQL = 
			"SELECT * FROM PURCHASE WHERE productID = ?";
	private static final String updateSQL =
			"UPDATE PURCHASE SET  purchaseDate = ?, purchaseAmount = ? WHERE id = ?";
	private static final String  deleteSQL =
			"DELETE FROM PURCHASE WHERE id = ?";
	
	/**
	 * The create method must throw a DAOException if the 
	 * given Purchase has a non-null ID. The create method must 
	 * return the same Purchase with the ID attribute set to the
	 * value set by the application's auto-increment primary key column. 
	 * @throws DAOException if the given Purchase has a non-null id.
	 */
	public Purchase create(Connection connection, Purchase purchase) throws SQLException, DAOException{
		if(purchase.getId() != null) {
			throw new DAOException("Trying to insert Purchase with NON-NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			//Compile query
			ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			ps.setLong(1, purchase.getProductID());
			ps.setLong(2, purchase.getCustomerID());
			ps.setDate(3, purchase.getPurchaseDate());
			ps.setDouble(4, purchase.getPurchaseAmount());
			
			//execute
			int res = ps.executeUpdate();
			//System.out.println(res);
			if(res != 1) {
				throw new DAOException("Did Not Create Expected Number Of Rows");
			}
			//retrieve auto-inc id and set it
			ResultSet krs = ps.getGeneratedKeys();
			krs.next();
			int autoID = krs.getInt(1);
			purchase.setId((long) autoID);
			return purchase;
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	/**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
	public Purchase retrieve(Connection connection, Long id) throws SQLException, DAOException{
		if(id == null) {
			throw new DAOException("Trying to retrieve Purchase with NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			//compile SQL
			ps = connection.prepareStatement(retrieveSQL);
			
			ps.setLong(1, id);
			
			//execute
			ResultSet rs = ps.executeQuery();
			//check if there is a result set
			if(!rs.next()) {
				return null;
			}
			//go to end of result set to see if it properly retrieved 1 thing
			rs.last();
			if(rs.getRow()!= 1) {
				throw new DAOException("Did Not Retrieve Expected Number of Rows");
			}
			else {
				//pull attributes in object
				Purchase purch = new Purchase();
				purch.setId(rs.getLong("id"));
				purch.setProductID(rs.getLong("productID"));
				purch.setCustomerID(rs.getLong("customerID"));
				purch.setPurchaseDate(rs.getDate("purchaseDate"));
				purch.setPurchaseAmount(rs.getDouble("purchaseAmount"));
				return purch;
			}
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
		
	}
	
	/**
	 * The update method must throw DAOException if the provided 
	 * Purchase has a NULL id. 
	 */
	public int update(Connection connection, Purchase purchase) throws SQLException, DAOException{
		if(purchase.getId() == null) {
			throw new DAOException("Trying to update Purchase with NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			//compile SQL
			ps = connection.prepareStatement(updateSQL);
			ps.setDate(1, purchase.getPurchaseDate());
			ps.setDouble(2, purchase.getPurchaseAmount());
			ps.setLong(3, purchase.getId());
			
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
	
	/**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
	public int delete(Connection connection, Long id) throws SQLException, DAOException{
		if(id == null) {
			throw new DAOException("Trying to delete Purchase with NULL ID");
		}
		PreparedStatement ps = null;
		try {
			//compile SQL
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
	
	/**
	 * Retrieve purchases for the given customer id
	 */
	public List<Purchase> retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException{
		List<Purchase> listPurch = new ArrayList<>();
		PreparedStatement ps = null;
		try {
			//compile SQL
			ps = connection.prepareStatement(retrieveCustSQL);
			//ps.setString(1, "customerID");
			ps.setLong(1, customerID);
			
			//execute
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				//add rows to arraylist
				Purchase purch = new Purchase();
				purch.setId(rs.getLong("id"));
				purch.setProductID(rs.getLong("productID"));
				purch.setCustomerID(rs.getLong("customerID"));
				purch.setPurchaseDate(rs.getDate("purchaseDate"));
				purch.setPurchaseAmount(rs.getDouble("purchaseAmount"));
				listPurch.add(purch);
			}
			return listPurch;
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	/**
	 * Retrieve purchases for the given product id
	 */
	public List<Purchase> retrieveForProductID(Connection connection, Long productID) throws SQLException, DAOException{
		List<Purchase> listPurch = new ArrayList<>();
		PreparedStatement ps = null;
		try {
			//SQL
			ps = connection.prepareStatement(retrieveProdSQL);
			//ps.setString(1, "productID");
			ps.setLong(1, productID);
			
			//execute
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				//add purchases to arraylist
				Purchase purch = new Purchase();
				purch.setId(rs.getLong("id"));
				purch.setProductID(rs.getLong("productID"));
				purch.setCustomerID(rs.getLong("customerID"));
				purch.setPurchaseDate(rs.getDate("purchaseDate"));
				purch.setPurchaseAmount(rs.getDouble("purchaseAmount"));
				listPurch.add(purch);
			}
			return listPurch;
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	/**
	 * Retrieve purchase summary for the given customer id
	 */
	public PurchaseSummary retrievePurchaseSummary(Connection connection, Long customerID) throws SQLException, DAOException{
		List<Purchase> purchases = new ArrayList<>();
		
		//do the SQL in a different method
		purchases = retrieveForCustomerID(connection, customerID);
		
		PurchaseSummary purchSum = new PurchaseSummary();
		int count = 0;
		double sum = 0;
		
		double min= Double.MAX_VALUE;
		double max= Double.MIN_VALUE;
		
		for(Purchase p: purchases) { //find max and min values as we loop through the result set, add values to a sum
			double amount = p.getPurchaseAmount();
			if(amount<min) {
				min = amount;
			}
			if(amount>max) {
				max = amount;
			}
			sum+= amount;
			count++;
		}
		
		double avg = sum/count; 
		purchSum.minPurchase = (float) min;
		purchSum.maxPurchase = (float) max;
		purchSum.avgPurchase = (float) avg;
		return purchSum;
	}
}
