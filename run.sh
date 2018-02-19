cd perproduct/
ssh gu_gateway 'rm perproduct_2.11-0.2.jar'
sbt package
scp target/scala-2.11/perproduct_2.11-0.2.jar gu_gateway:/home/snikitin/

