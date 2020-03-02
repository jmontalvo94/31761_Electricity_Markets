using DataFrames
using Plots
using JuMP
using Gurobi
using MathOptFormat
using LinearAlgebra
using Printf
using CSV
using Dates
gr()

#Read data
files=["consumption-prognosis_2019_hourly.csv","consumption-prognosis_2020_hourly.csv","wind-power-dk-prognosis_2019_hourly.csv","wind-power-dk-prognosis_2020_hourly.csv"]

function readandclean(file)
    df = CSV.read(file,header=3,datarow=4)
    df = rename!(df,:1=>"Date")
    return df
end

df_consumption2019 = readandclean(files[1])
df_consumption2020 = readandclean(files[2])
df_wind2019 = readandclean(files[3])
df_wind2020 = readandclean(files[4])
#check for missing values

#=Data to arrays?
array_b = Array(df_b)
b = zeros(Int,length(M),length(F),length(T),length(S))
for i=1:size(array_b,1)
    b[array_b[i,1],array_b[i,2],array_b[i,3],array_b[i,4]]=array_b[i,5]
end
=#

consumer = ["WeLovePower","CleanCharge","JyskeEl","ElRetail","QualiWatt","IntelliWatt", "El-Forbundet"]
id_D = ["D₁","D₂","D₃","D₄","D₅","D₆","D₇"]
en_import = 100
# Initialize vectors
N_D = size(consumer,1)
n_D = collect(1:N_D)
n = collect(1:(N_G+N_D))
P_D  = [35,23,12,38,43,16,57]
λ_D = [65,78,10,46,63,32,50]
c = vcat(λ_G,-λ_D)
A_eq = transpose(vcat(ones(N_G),-ones(N_D)))
A = Array(Diagonal(ones(N_G+N_D)))
b = vcat(P_G,P_D)
b_eq = 0

# Model
model_supplydemand = Model(with_optimizer(Gurobi.Optimizer))
@variable(model_supplydemand, y[j in n] >= 0)
@objective(model_supplydemand, Min, transpose(c)*y)
@constraint(model_supplydemand, generation, A*y .<= b)
@constraint(model_supplydemand, balance, A_eq*y == b_eq)
optimize!(model_supplydemand)

# Model output
if termination_status(model_supplydemand) == MOI.OPTIMAL
    println("Optimal solution found!\n")
    println("Generation and Demand:\n")
    for j in n_G
        println("$(id_G[j]): ", value.(y[j]), " MWh")
    end
    for i in n_D
        println("$(id_D[i]): ", value.(y[i]), " MWh")
    end
    println("\nObjective value: ", objective_value(model_supplydemand), " €")
    println("\nMarket equilibrium: ", dual(balance), " €/MWh")
    else
        error("No solution.")
end

power = Array(n)
for k in n
    power[k]=value.(y)[k]
end

# Data Frames
df_G = DataFrame(Supplier=supplier,ID_G=id_G,Offer_G=P_G,Price_G=λ_G,Schedule_G=power[1:N_G],Market_G=fill(dual(balance),N_G))
df_D = DataFrame(Consumer=consumer,ID_D=id_D,Offer_D=P_D,Price_D=λ_D,Schedule_D=power[N_G+1:maximum(n)],Market_D=fill(dual(balance),N_D))
df_G.PayAsBid_G = df_G.Schedule_G.*df_G.Price_G
df_G.UniformPricing_G = df_G.Schedule_G.*df_G.Market_G
df_D.PayAsBid_D = df_D.Schedule_D.*df_D.Price_D
df_D.UniformPricing_D = df_D.Schedule_D.*df_D.Market_D

#plot(bidsSupplier.AggregatedQ,bidsSupplier.Price,w=2,t=:steppre, xlim=(0,sum(bidsSupplier.Quantity)), xlab="Quantity [MWh]", ylab="Price [EUR/MWh]", color="darkred", legend=false)
