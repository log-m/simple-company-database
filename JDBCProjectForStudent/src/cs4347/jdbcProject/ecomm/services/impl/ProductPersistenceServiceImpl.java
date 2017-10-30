package cs4347.jdbcProject.ecomm.services.impl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import cs4347.jdbcProject.ecomm.services.ProductPersistenceService;
import cs4347.jdbcProject.ecomm.dao.ProductDAO;
import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.util.DAOException;
import cs4347.jdbcProject.ecomm.dao.impl.ProductDaoImpl;

public class ProductPersistenceServiceImpl implements ProductPersistenceService
{
	private DataSource dataSource;

	public ProductPersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}
	
	public Product create(Product product) throws SQLException, DAOException
	{
		ProductDAO prodDAO = new ProductDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			Product prod = prodDAO.create(conn, product);
			//Long prodID = prod.getId();
			conn.commit();
			return prod;
			
		}
		catch (Exception ex) {
			conn.rollback();
			throw ex;
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
			}
			if(conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}
	public Product retrieve(Long id) throws SQLException, DAOException{
		ProductDAO prodDAO = new ProductDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			Product prod = prodDAO.retrieve(conn, id); //throw null ID DAOException in here
			conn.commit();
			return prod;
		}
		catch(Exception ex) {
			conn.rollback();
			throw ex;
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
			}
			if(conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}
	
	public int update(Product product) throws SQLException, DAOException{
		ProductDAO prodDAO = new ProductDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			int count = prodDAO.update(conn, product); //throw null ID DAOException in here
			conn.commit();
			return count;
		}
		catch(Exception ex) {
			conn.rollback();
			throw ex;
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
			}
			if(conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}
	
	public int delete(Long id) throws SQLException, DAOException{
		ProductDAO prodDAO = new ProductDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			int count = prodDAO.delete(conn, id); //throw null ID DAOException in here
			conn.commit();
			return count;
		}
		catch(Exception ex) {
			conn.rollback();
			throw ex;
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
			}
			if(conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}
	
	public Product retrieveByUPC(String upc) throws SQLException, DAOException{
		ProductDAO prodDAO = new ProductDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			Product prod = prodDAO.retrieveByUPC(conn, upc); //throw null ID DAOException in here
			conn.commit();
			return prod;
		}
		catch(Exception ex) {
			conn.rollback();
			throw ex;
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
			}
			if(conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}
	
	public List<Product> retrieveByCategory(int category) throws SQLException, DAOException{
		ProductDAO prodDAO = new ProductDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			List<Product> prods = prodDAO.retrieveByCategory(conn, category); //throw null ID DAOException in here
			conn.commit();
			return prods;
		}
		catch(Exception ex) {
			conn.rollback();
			throw ex;
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
			}
			if(conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}

}
	
