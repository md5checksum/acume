package acume.exception

class AcumeException(code : String, cause: Throwable) extends RuntimeException(code, cause) {


	def this(code : String) {
		this(code, null)
	}


	def this(code : String, message : String ,  cause : Throwable) {
		this(code + ": " + message, cause)
	}

	def  getCode() = {
		this.code;
	}
}