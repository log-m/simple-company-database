package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.dao.impl.AddressDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CreditCardDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CustomerDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.services.CustomerPersistenceService;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerPersistenceServiceImpl implements CustomerPersistenceService
{
	private DataSource dataSource;

	public CustomerPersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}
	
	/**
	 * This method provided as an example of transaction support across multiple inserts.
	 * 
	 * Persists a new Customer instance by inserting new Customer, Address, 
	 * and CreditCard instances. Notice the transactional nature of this 
	 * method which inludes turning off autocommit at the start of the 
	 * process, and rolling back the transaction if an exception 
	 * is caught. 
	 */
	@Override
	public Customer create(Customer customer) throws SQLException, DAOException
	{
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			Customer cust = customerDAO.create(connection, customer);
			Long custID = cust.getId();

			if (cust.getAddress() == null) {
				throw new DAOException("Customers must include an Address instance.");
			}
			Address address = cust.getAddress();
			addressDAO.create(connection, address, custID);

			if (cust.getCreditCard() == null) {
				throw new DAOException("Customers must include an CreditCard instance.");
			}
			CreditCard creditCard = cust.getCreditCard();
			creditCardDAO.create(connection, creditCard, custID);

			connection.commit();
			return cust;
		}
		catch (Exception ex) {
			connection.rollback();
			throw ex;
		}
		finally {
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

public Customer retrieve(Long id) throws SQLException, DAOException{
		CustomerDAO custDAO = new CustomerDaoImpl();
		AddressDAO aDAO = new AddressDaoImpl();
		CreditCardDAO cDAO = new CreditCardDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			Customer cust = custDAO.retrieve(conn, id); //throw null ID DAOException in here
			
			//retrieve corresponding address and credit card instances to make a complete customer object
			Address add = aDAO.retrieveForCustomerID(conn, id); 
			CreditCard cred = cDAO.retrieveForCustomerID(conn, id);
			cust.setAddress(add);
			cust.setCreditCard(cred);
			conn.commit();
			return cust;
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
	
	public int update(Customer customer) throws SQLException, DAOException{
		CustomerDAO custDAO = new CustomerDaoImpl();
		AddressDAO aDAO = new AddressDaoImpl();
		CreditCardDAO cDAO = new CreditCardDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			int count = custDAO.update(conn, customer); //throw null ID DAOException in here
			
			//There are no update methods for creditcard and address, so just delete to old ones and create the new ones. assumption is that each customer only has 1 card and 1 address
			aDAO.deleteForCustomerID(conn,customer.getId());
			aDAO.create(conn, customer.getAddress(),customer.getId());
			cDAO.deleteForCustomerID(conn, customer.getId());
			cDAO.create(conn, customer.getCreditCard(), customer.getId());
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
		CustomerDAO custDAO = new CustomerDaoImpl();
		
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			int count = custDAO.delete(conn, id); //throw null ID DAOException in here
			//creditcard and address are set to cascade delete, so we don't need to call the delete methods here
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
	
	public List<Customer> retrieveByZipCode(String zipCode) throws SQLException, DAOException{
		CustomerDAO custDAO = new CustomerDaoImpl();
		AddressDAO aDAO = new AddressDaoImpl();
		CreditCardDAO cDAO = new CreditCardDaoImpl();
		Connection conn = dataSource.getConnection();
		
		List<Customer> custList;
		try {
			conn.setAutoCommit(false);
			 custList = custDAO.retrieveByZipCode(conn, zipCode); //throw null ID DAOException in here
			 for(int i = 0; i < custList.size(); i++) {
				 //match each customer retrieved with their address and credit card
				 Address add = aDAO.retrieveForCustomerID(conn, custList.get(i).getId());
				CreditCard cred = cDAO.retrieveForCustomerID(conn, custList.get(i).getId());
				custList.get(i).setAddress(add);
				custList.get(i).setCreditCard(cred);
			 }
			conn.commit();
			return custList;
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
	
	public List<Customer> retrieveByDOB(Date startDate, Date endDate) throws SQLException, DAOException{
		CustomerDAO custDAO = new CustomerDaoImpl();
		AddressDAO aDAO = new AddressDaoImpl();
		CreditCardDAO cDAO = new CreditCardDaoImpl();
		Connection conn = dataSource.getConnection();
		try {
			conn.setAutoCommit(false);
			List<Customer> custs = custDAO.retrieveByDOB(conn, startDate, endDate); //throw null ID DAOException in here
			for(int i = 0; i < custs.size(); i++) {
				//match each customer retrieved with their address and credit card
				 Address add = aDAO.retrieveForCustomerID(conn, custs.get(i).getId());
				CreditCard cred = cDAO.retrieveForCustomerID(conn, custs.get(i).getId());
				custs.get(i).setAddress(add);
				custs.get(i).setCreditCard(cred);
			 }
			conn.commit();
			return custs;
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

