package cs4347.jdbcProject.ecomm.services.impl;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.dao.impl.PurchaseDaoImpl;
import cs4347.jdbcProject.ecomm.services.PurchasePersistenceService;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class PurchasePersistenceServiceImpl implements PurchasePersistenceService
{
	private DataSource dataSource;

	public PurchasePersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}
	/**
	 * The create method must throw a DAOException if the 
	 * given Purchase has a non-null ID. The create method must 
	 * return the same Purchase with the ID attribute set to the
	 * value set by the application's auto-increment primary key column. 
	 * @throws DAOException if the given Purchase has a non-null id.
	 */
	public Purchase create(Purchase purchase) throws SQLException, DAOException{
		PurchaseDAO purchDAO = new PurchaseDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			Purchase purch = purchDAO.create(conn, purchase);
			conn.commit();
			return purch;
			
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
	
	/**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
	public Purchase retrieve(Long id) throws SQLException, DAOException{
		PurchaseDAO purchDAO = new PurchaseDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			Purchase purch = purchDAO.retrieve(conn, id); //throw null ID DAOException in here
			conn.commit();
			return purch;
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
	
	/**
	 * The update method must throw DAOException if the provided 
	 * Purchase has a NULL id. 
	 */
	public int update(Purchase purchase) throws SQLException, DAOException{
		PurchaseDAO purchDAO = new PurchaseDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			int count = purchDAO.update(conn, purchase); //throw null ID DAOException in here
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
	
	/**
	 * The update method must throw DAOException if the provided 
	 * ID is null. 
	 */
	public int delete(Long id) throws SQLException, DAOException{
		PurchaseDAO purchDAO = new PurchaseDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			int count = purchDAO.delete(conn, id); //throw null ID DAOException in here
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
	
	/**
	 * Retrieve purchases made by the given customer.
	 */
	public List<Purchase> retrieveForCustomerID(Long customerID) throws SQLException, DAOException{
		PurchaseDAO purchDAO = new PurchaseDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			List<Purchase> results = purchDAO.retrieveForCustomerID(conn, customerID); //throw null ID DAOException in here
			conn.commit();
			return results;
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
	
	/**
	 * Produce a purchase summary report for the given customer.
	 */
	public PurchaseSummary retrievePurchaseSummary(Long customerID) throws SQLException, DAOException{
		PurchaseDAO purchDAO = new PurchaseDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			PurchaseSummary summ = purchDAO.retrievePurchaseSummary(conn, customerID); //throw null ID DAOException in here
			conn.commit();
			return summ;
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

	/**
	 * Retrieve purchases made for the given product.
	 */
	public List<Purchase> retrieveForProductID(Long productID) throws SQLException, DAOException{
		PurchaseDAO purchDAO = new PurchaseDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			List<Purchase> results = purchDAO.retrieveForCustomerID(conn, productID); //throw null ID DAOException in here
			conn.commit();
			return results;
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
