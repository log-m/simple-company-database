package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.ProductDAO;
import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class ProductDaoImpl implements ProductDAO
{
	private static final String insertSQL = 
			"INSERT INTO PRODUCT (prodName, prodDescription, prodCategory, prodUPC"
			+ "VALUES(?, ?, ?, ?)";
	private static final String retrieveSQL = 
			"SELECT * FROM PRODUCT WHERE ? = ?";
	private static final String updateSQL =
			"UPDATE PRODUCT SET prodName = ?, prodDescription = ?, prodCategory = ?, prodUPC = ? WHERE id = ?";
	private static final String  deleteSQL =
			"DELETE FROM PRODUCT WHERE id = ?";
	public Product create(Connection connection, Product product) throws SQLException, DAOException
	{
		if(product.getId() != null) {
			throw new DAOException("Trying to insert Product with NON-NULL ID");
		}
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, product.getProdName());
			ps.setString(2, product.getProdDescription());
			ps.setInt(3, product.getProdCategory());
			ps.setString(4, product.getProdUPC());
			
			int res = ps.executeUpdate();
			if(res != 1) {
				throw new DAOException("Did Not Create Expected Number Of Rows");
			}
			ResultSet krs = ps.getGeneratedKeys();
			krs.next();
			int autoID = krs.getInt(1);
			product.setId((long) autoID);
			return product;
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public Product retrieve(Connection connection, Long id) throws SQLException, DAOException{
		if(id == null) {
			throw new DAOException("Trying to retrieve Product with NULL ID");
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
				Product prod = new Product();
				prod.setId(rs.getLong("id"));
				prod.setProdName(rs.getString("prodName"));
				prod.setProdDescription(rs.getString("prodDescription"));
				prod.setProdCategory(rs.getInt("prodCategory"));
				prod.setProdUPC(rs.getString("prodUPC"));
				return prod;
					  
			}
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	public List<Product> retrieveByCategory(Connection connection, int cat) throws SQLException, DAOException{
		
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retrieveSQL);
			ps.setString(1, "prodCategory");
			ps.setInt(2, cat);
			
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			rs.beforeFirst();
			List<Product> prodList = new ArrayList<Product>();
			while(rs.next()) {
				Product prod = new Product();
				prod.setId(rs.getLong("id"));
				prod.setProdDescription(rs.getString("prodDescription"));
				prod.setProdCategory(rs.getInt("prodCategory"));
				prod.setProdUPC(rs.getString("prodUPC"));
				prodList.add(prod);
					  
			}
			return prodList;
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	
	public Product retrieveByUPC(Connection connection, String UPC) throws SQLException, DAOException{
		if(UPC == null) {
			throw new DAOException("Trying to retrieve Product with NULL UPC code");
		}
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(retrieveSQL);
			ps.setString(1, "prodUPC");
			ps.setString(2, UPC);
			
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			rs.last();
			if(rs.getRow() != 1) {
					  throw new DAOException("Did Not Retrieve Expected Number Of Rows");
			}
			else {
				Product prod = new Product();
				prod.setId(rs.getLong("id"));
				prod.setProdDescription(rs.getString("prodDescription"));
				prod.setProdCategory(rs.getInt("prodCategory"));
				prod.setProdUPC(rs.getString("prodUPC"));
				return prod;
					  
			}
			
			
		}
		finally {
			if(ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}
	public Product update(Connection connection, Long id) throws SQLException, DAOException{
		if(id == null) {
			throw new DAOException("Trying to update Product with NULL ID");
		}
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(updateSQL);
			ps.setString(1, "prodName");
			ps.setString(2, "prodDescription");
			ps.setInt(3, "prodCategory");
			ps.setString(4,"prodUPC");
			ps.setLong(5, id);
			
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
			throw new DAOException("Trying to delete Product with NULL ID");
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
